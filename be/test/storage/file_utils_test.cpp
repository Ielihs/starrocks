// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/file_utils_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/file_utils.h"

#include <filesystem>
#include <fstream>
#include <set>
#include <vector>

#include "common/configbase.h"
#include "env/env.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/olap_define.h"
#include "util/logging.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace starrocks {

class FileUtilsTest : public testing::Test {
public:
    void SetUp() override { ASSERT_TRUE(std::filesystem::create_directory(_s_test_data_path)); }

    void TearDown() override { ASSERT_TRUE(std::filesystem::remove_all(_s_test_data_path)); }

    static std::string _s_test_data_path;
};

std::string FileUtilsTest::_s_test_data_path = "./file_utils_testxxxx123";

void save_string_file(const std::filesystem::path& p, const std::string& str) {
    std::ofstream file;
    file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    file.open(p, std::ios_base::binary);
    file.write(str.c_str(), str.size());
};

TEST_F(FileUtilsTest, TestCopyFile) {
    std::string src_file_name = _s_test_data_path + "/abcd12345.txt";
    std::unique_ptr<WritableFile> src_file = *Env::Default()->new_writable_file(src_file_name);

    char large_bytes2[(1 << 12)];
    memset(large_bytes2, 0, sizeof(char) * ((1 << 12)));
    int i = 0;
    while (i < 1 << 10) {
        ASSERT_TRUE(src_file->append(Slice(large_bytes2, 1 << 12)).ok());
        ++i;
    }
    ASSERT_TRUE(src_file->append(Slice(large_bytes2, 13)).ok());
    ASSERT_TRUE(src_file->close().ok());

    std::string dst_file_name = _s_test_data_path + "/abcd123456.txt";
    FileUtils::copy_file(src_file_name, dst_file_name);

    ASSERT_EQ(4194317, std::filesystem::file_size(dst_file_name));
}

TEST_F(FileUtilsTest, TestRemove) {
    // remove_all
    ASSERT_TRUE(FileUtils::remove_all("./file_test").ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test"));

    ASSERT_TRUE(FileUtils::create_dir("./file_test/123/456/789").ok());
    ASSERT_TRUE(FileUtils::create_dir("./file_test/abc/def/zxc").ok());
    ASSERT_TRUE(FileUtils::create_dir("./file_test/abc/123").ok());

    save_string_file("./file_test/s1", "123");
    save_string_file("./file_test/123/s2", "123");

    ASSERT_TRUE(FileUtils::check_exist("./file_test"));
    ASSERT_TRUE(FileUtils::remove_all("./file_test").ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test"));

    // remove
    ASSERT_TRUE(FileUtils::create_dir("./file_test/abc/123").ok());
    save_string_file("./file_test/abc/123/s2", "123");

    ASSERT_FALSE(FileUtils::remove("./file_test").ok());
    ASSERT_FALSE(FileUtils::remove("./file_test/abc/").ok());
    ASSERT_FALSE(FileUtils::remove("./file_test/abc/123").ok());

    ASSERT_TRUE(FileUtils::check_exist("./file_test/abc/123/s2"));
    ASSERT_TRUE(FileUtils::remove("./file_test/abc/123/s2").ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test/abc/123/s2"));

    ASSERT_TRUE(FileUtils::check_exist("./file_test/abc/123"));
    ASSERT_TRUE(FileUtils::remove("./file_test/abc/123/").ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test/abc/123"));

    ASSERT_TRUE(FileUtils::remove_all("./file_test").ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test"));

    // remove paths
    ASSERT_TRUE(FileUtils::create_dir("./file_test/123/456/789").ok());
    ASSERT_TRUE(FileUtils::create_dir("./file_test/abc/def/zxc").ok());
    save_string_file("./file_test/s1", "123");
    save_string_file("./file_test/s2", "123");

    std::vector<std::string> ps;
    ps.push_back("./file_test/123/456/789");
    ps.push_back("./file_test/123/456");
    ps.push_back("./file_test/123");

    ASSERT_TRUE(FileUtils::check_exist("./file_test/123"));
    ASSERT_TRUE(FileUtils::remove_paths(ps).ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test/123"));

    ps.clear();
    ps.push_back("./file_test/s1");
    ps.push_back("./file_test/abc/def");

    ASSERT_FALSE(FileUtils::remove_paths(ps).ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test/s1"));
    ASSERT_TRUE(FileUtils::check_exist("./file_test/abc/def/"));

    ps.clear();
    ps.push_back("./file_test/abc/def/zxc");
    ps.push_back("./file_test/s2");
    ps.push_back("./file_test/abc/def");
    ps.push_back("./file_test/abc");

    ASSERT_TRUE(FileUtils::remove_paths(ps).ok());
    ASSERT_FALSE(FileUtils::check_exist("./file_test/s2"));
    ASSERT_FALSE(FileUtils::check_exist("./file_test/abc"));

    ASSERT_TRUE(FileUtils::remove_all("./file_test").ok());
}

TEST_F(FileUtilsTest, TestCreateDir) {
    // normal
    std::string path = "./file_test/123/456/789";
    FileUtils::remove_all("./file_test");
    ASSERT_FALSE(FileUtils::check_exist(path));

    ASSERT_TRUE(FileUtils::create_dir(path).ok());

    ASSERT_TRUE(FileUtils::check_exist(path));
    ASSERT_TRUE(FileUtils::is_dir("./file_test"));
    ASSERT_TRUE(FileUtils::is_dir("./file_test/123"));
    ASSERT_TRUE(FileUtils::is_dir("./file_test/123/456"));
    ASSERT_TRUE(FileUtils::is_dir("./file_test/123/456/789"));

    FileUtils::remove_all("./file_test");

    // normal
    path = "./file_test/123/456/789/";
    FileUtils::remove_all("./file_test");
    ASSERT_FALSE(FileUtils::check_exist(path));

    ASSERT_TRUE(FileUtils::create_dir(path).ok());

    ASSERT_TRUE(FileUtils::check_exist(path));
    ASSERT_TRUE(FileUtils::is_dir("./file_test"));
    ASSERT_TRUE(FileUtils::is_dir("./file_test/123"));
    ASSERT_TRUE(FileUtils::is_dir("./file_test/123/456"));
    ASSERT_TRUE(FileUtils::is_dir("./file_test/123/456/789"));

    FileUtils::remove_all("./file_test");

    // absolute path;
    std::string real_path;
    Env::Default()->canonicalize(".", &real_path);
    ASSERT_TRUE(FileUtils::create_dir(real_path + "/file_test/absolute/path/123/asdf").ok());
    ASSERT_TRUE(FileUtils::is_dir("./file_test/absolute/path/123/asdf"));
    FileUtils::remove_all("./file_test");
}

TEST_F(FileUtilsTest, TestListDirsFiles) {
    std::string path = "./file_test/";
    FileUtils::remove_all(path);
    FileUtils::create_dir("./file_test/1");
    FileUtils::create_dir("./file_test/2");
    FileUtils::create_dir("./file_test/3");
    FileUtils::create_dir("./file_test/4");
    FileUtils::create_dir("./file_test/5");

    std::set<string> dirs;
    std::set<string> files;

    ASSERT_TRUE(FileUtils::list_dirs_files("./file_test", &dirs, &files).ok());
    ASSERT_EQ(5, dirs.size());
    ASSERT_EQ(0, files.size());

    dirs.clear();
    files.clear();

    ASSERT_TRUE(FileUtils::list_dirs_files("./file_test", &dirs, nullptr).ok());
    ASSERT_EQ(5, dirs.size());
    ASSERT_EQ(0, files.size());

    save_string_file("./file_test/f1", "just test");
    save_string_file("./file_test/f2", "just test");
    save_string_file("./file_test/f3", "just test");

    dirs.clear();
    files.clear();

    ASSERT_TRUE(FileUtils::list_dirs_files("./file_test", &dirs, &files).ok());
    ASSERT_EQ(5, dirs.size());
    ASSERT_EQ(3, files.size());

    dirs.clear();
    files.clear();

    ASSERT_TRUE(FileUtils::list_dirs_files("./file_test", nullptr, &files).ok());
    ASSERT_EQ(0, dirs.size());
    ASSERT_EQ(3, files.size());

    FileUtils::remove_all(path);
}
} // namespace starrocks
