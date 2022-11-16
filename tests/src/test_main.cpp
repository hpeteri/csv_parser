#define N1_CSV_IMPLEMENTATION
#include "n1_csv_parser.h"

#include "unittest/n1_unittest.h"

TEST(n1_OpenCSV_Test){
  TEST_SUCCESS();
}

int main(){

  auto parser = n1_create_csv_parser("test_0.csv");
  n1_destroy_csv_parser(parser);
  
  return 0;
}
