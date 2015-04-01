// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>

#include "ScrubStack.h"
#include "mds/MDS.h"
#include "mds/MDCache.h"
#include "mds/MDSContinuation.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, scrubstack->mdcache->mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".scrubstack ";
}

void ScrubStack::push_dentry(CDentry *dentry)
{
  dout(20) << "pushing " << *dentry << " on top of ScrubStack" << dendl;
  dentry->get(CDentry::PIN_SCRUBQUEUE);
  dentry_stack.push_front(&dentry->item_scrub);
}

void ScrubStack::push_dentry_bottom(CDentry *dentry)
{
  dout(20) << "pushing " << *dentry << " on bottom of ScrubStack" << dendl;
  dentry->get(CDentry::PIN_SCRUBQUEUE);
  dentry_stack.push_back(&dentry->item_scrub);
}

void ScrubStack::pop_dentry(CDentry *dn)
{
  dout(20) << "popping " << *dn
          << " off of ScrubStack" << dendl;
  assert(!dn->item_scrub.is_on_list());
  dn->put(CDentry::PIN_SCRUBQUEUE);
  dn->item_scrub.remove_myself();
}

void ScrubStack::_enqueue_dentry(CDentry *dn, bool recursive, bool children,
                                 MDSInternalContextBase *on_finish, bool top)
{
  dout(10) << __func__ << " with {" << *dn << "}"
           << ", recursive=" << recursive << ", children=" << children
           << ", on_finish=" << on_finish << ", top=" << top << dendl;
  assert(mdcache->mds->mds_lock.is_locked_by_me());
  dn->scrub_initialize(NULL, recursive, children, on_finish);
  if (top)
    push_dentry(dn);
  else
    push_dentry_bottom(dn);
}
void ScrubStack::enqueue_dentry(CDentry *dn, bool recursive, bool children,
                                 MDSInternalContextBase *on_finish, bool top)
{
  _enqueue_dentry(dn, recursive, children, on_finish, top);
  kick_off_scrubs();
}

void ScrubStack::kick_off_scrubs()
{
  bool can_continue = true;
  elist<CDentry*>::iterator i = dentry_stack.begin();
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
      can_continue && !i.end()) {
    CDentry *cur = *i;
    CInode *curi = cur->get_projected_inode();
    ++i; // we have our reference, push iterator forward

    if (!curi->is_dir()) {
      // it's a regular file, symlink, or hard link
      pop_dentry(cur); // we only touch it this once, so remove from stack

      if (curi->is_file()) {
        scrub_file_dentry(cur);
        can_continue = true;
      } else {
        // drat, we don't do anything with these yet :(
        dout(5) << "skipping scrub on non-dir, non-file dentry "
                << *cur << dendl;
      }
    } else {
      bool completed; // it's done, so pop it off the stack
      bool terminal; // not done, but we can start ops on other directories
      bool progress; // it added new dentries to the top of the stack
      scrub_dir_dentry(cur, &progress, &terminal, &completed);
      if (completed) {
        pop_dentry(cur);
      } else if (progress) {
        // we added new stuff to top of stack, so reset ourselves there
        i = dentry_stack.begin();
      }

      can_continue = progress || terminal || completed;
    }
  }
}

void ScrubStack::scrub_dir_dentry(CDentry *dn,
                                  bool *added_children,
                                  bool *terminal,
                                  bool *done)
{
  dout(10) << __func__ << *dn << dendl;

  if (!dn->scrub_info()->scrub_children &&
      !dn->scrub_info()->scrub_recursive) {
    // TODO: we have to scrub the local dentry/inode, but nothing else
  }

  *added_children = false;
  *terminal = false;
  *done = false;

  CInode *in = dn->get_projected_inode();
  list<frag_t> scrubbing_frags;
  list<CDir*> scrubbing_cdirs;
  in->scrub_dirfrags_scrubbing(&scrubbing_frags);
  for (list<frag_t>::iterator i = scrubbing_frags.begin();
      i != scrubbing_frags.end();
      ++i) {
    // turn frags into CDir *
    CDir *dir = in->get_dirfrag(*i);
    scrubbing_cdirs.push_back(dir);
    dout(25) << "got CDir " << *dir << " presently scrubbing" << dendl;
  }


  list<CDir*>::iterator i = scrubbing_cdirs.begin();
  bool all_frags_terminal = true;
  bool all_frags_done = true;
  bool finally_done = false;
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress) {
    // select next CDir
    CDir *cur_dir = NULL;
    if (i != scrubbing_cdirs.end()) {
      cur_dir = *i;
      ++i;
    } else {
      bool ready = get_next_cdir(in, &cur_dir);
      if (ready && cur_dir) {
        cur_dir->scrub_initialize();
        scrubbing_cdirs.push_back(cur_dir);
      } else {
        goto frags_finished;
      }
    }
    // scrub that CDir
    bool frag_added_children = false;
    bool frag_terminal = true;
    bool frag_done = false;
    scrub_dirfrag(cur_dir, &frag_added_children, &frag_terminal, &frag_done);
    *added_children |= frag_added_children;
    all_frags_terminal = all_frags_terminal && frag_terminal;
    all_frags_done = all_frags_done && frag_done;
  }
frags_finished:
  dout(20) << "finished looping; all_frags_terminal=" << all_frags_terminal
           << ", all_frags_done=" << all_frags_done << dendl;
  if (all_frags_done) {
    assert (!*added_children); // can't do this if children are still pending
    scrub_dir_dentry_final(dn, &finally_done);
  }

  *terminal = all_frags_terminal;
  *done = all_frags_done && finally_done;
  dout(10) << __func__ << " is exiting" << dendl;
  return;
}

bool ScrubStack::get_next_cdir(CInode *in, CDir **new_dir)
{
  dout(20) << __func__ << " on " << *in << dendl;
  frag_t next_frag;
  int r = in->scrub_dirfrag_next(&next_frag);
  assert (r >= 0);

  if (r == 0) {
    // we got a frag to scrub, otherwise it would be ENOENT
    dout(25) << "looking up new frag " << next_frag << dendl;
    CDir *next_dir = in->get_or_open_dirfrag(mdcache, next_frag);
    if (!next_dir->is_complete()) {
      C_KickOffScrubs *c = new C_KickOffScrubs(this);
      next_dir->fetch(c);
      dout(25) << "fetching frag from RADOS" << dendl;
      return false;
    }
    *new_dir = next_dir;
    dout(25) << "returning dir " << *new_dir << dendl;
    return true;
  }
  assert(r == ENOENT);
  // there are no dirfrags left
  *new_dir = NULL;
  return true;
}



#if 0
void ScrubStack::kick_off_scrubs()
{
  dout(10) << __func__ << dendl;
  assert(mdcache->mds->mds_lock.is_locked_by_me());

  bool can_continue = true;
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
      can_continue &&
      !dentry_stack.empty()) {
    CDentry *top = peek_dentry();
    CInode *topi = top->get_projected_inode();
    if (!topi->is_dir()) {
      // okay, we've got a regular inode, a symlink, or a hardlink
      pop_dentry(); // we only touch it this once, so remove it from the stack

      if (topi->is_file()) {
        scrub_file_dentry(top);
        can_continue = true;
      } else {
        // drat, we don't do anything with these yet :(
        dout(5) << "skipping scrub on non-dir, non-file dentry " << *top
                << dendl;
      }
    } else {
      bool completed; // it's done, so pop it off the top of the stack
      bool terminal; // not done, but we can start ops on other directories
      bool progress; // it added new dentries to the top of the stack
      scrub_dir_dentry(top, &progress, &terminal, &completed);
      if (completed) {
        pop_dentry();
      }
      can_continue = progress || completed;
    }
  }

  dout(10) << __func__ << " is exiting" << dendl;
}


#endif
