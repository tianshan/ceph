#include <stdio.h>
#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include "osd/ECBackend.h"

using namespace std;

void test_overwrite_info()
{
  // map<int, int> a;
  // a.insert(make_pair(1,1));
  // a.insert(make_pair(2, 2));
  // bufferlist testbl;
  // ::encode(a, testbl);
  // string value(testbl.c_str(), testbl.length());
  // cout << value << std::endl;
  ifstream t("/root/out");
  bufferlist bl;
  string ss((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
  // string ss("\001\001\764\000\000\000\012\000\000\000\014\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000\017\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000\022\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000\025\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000\030\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000\033\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000\036\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000!\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000$\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\020\000\000\000\000\000\000(\000\000\000\000\000\000\000\000 \000\000\000\000\000\000\000\020\000\000\000\000\000\000");
  cout<< ss.length() << std::endl;
  bl.copy_in(0, ss.length(), ss.c_str());
  cout<< " copy end" << std::endl;

  // ECBackend::OverwriteInfo ow_info;
  // ow_info.decode(bl.begin());
  map<version_t, pair<uint64_t, uint64_t> > m;
  // ::decode(m, bl.begin());
  for (map<version_t, pair<uint64_t, uint64_t> >::iterator i = 
          m.begin();
        i != m.end();
        ++i) {
    cout<< i->second << std::endl;
  }
}

void test_sinfo_offset(uint64_t off, uint64_t len)
{
  const ECUtil::stripe_info_t sinfo(2, 8192);

  pair<uint64_t, uint64_t> to_write = make_pair(off, len);
  map<int, pair<uint64_t, uint64_t>> shards_to_write =
      sinfo.offset_len_to_chunk_offset(to_write, 3);
  cout<< "test: off " << off << " len " << len << std::endl;
  for (int i = 0; i < 3; ++i) {
    cout<< shards_to_write[i] << std::endl;
  }
}

void test_map_length()
{
  map<uint64_t, pair<uint64_t, uint64_t> > m;
  for (int i=0; i<5; ++i) {
    m.insert(make_pair(i, make_pair(i, i)));
    bufferlist bl;
    ::encode(m, bl);
    cout<< bl.length() << std::endl;
  }
}

int main()
{
  test_sinfo_offset(585, 64); 
  test_sinfo_offset(4096, 4096);
  test_sinfo_offset(4090, 10);

  test_sinfo_offset(8192, 4096);
  test_sinfo_offset(8190, 10);
  test_sinfo_offset(8190, 4100);

  // test_map_length();
  // test_overwrite_info();
  return 0;
}
