#define N1_CSV_IMPLEMENTATION
#include "n1_csv_parser.h"

#include "unittest/n1_unittest.h"

#define N1_TIMESTAMP_IMPLEMENTATION
#include "timestamp/n1_timestamp.h"

#define PRINT_LOG_TABLE_HEADER(){printf("File | Columns | Rows | Cells | Info | Time (ms) | MBps \n ---|---|---|---|---|---|---\n");} 
#define PRINT_LOG_PARSER(name, parser, info, time) {printf("%s | %u |  %u | %ld | '%s' |  %f | %f \n", name, parser->column_count, parser->row_count, parser->cell_count, info, time / 1000.0, (parser->file_size / 1024.0 / 1024.0) / (time / 1000000.0));}

void test_csv(const char* filename, void (*parsefunc)(n1_CSV_Parser* parser, char delim, char quote, char newline), const char* info){
  
  uint64_t start = n1_gettimestamp_microseconds();
  auto parser = n1_create_csv_parser(filename);
  parsefunc(parser, ',', '"', '\n');

  uint64_t end = n1_gettimestamp_microseconds();
  double time = (end - start);
    
  PRINT_LOG_PARSER(filename, parser, info, time);
  n1_destroy_csv_parser(parser);
}

int main(){
  const char* filenames[] = {
    "test_data/vehicles.csv",
    "test_data/2015_StateDepartment.csv",
  };

  PRINT_LOG_TABLE_HEADER();
  for(auto filename : filenames){
    test_csv(filename, n1_csv_parse_slow, "slow");
    test_csv(filename, n1_csv_parse_threaded_slow, "slow threaded");
    test_csv(filename, n1_csv_parse_threaded_sse2, "sse2 threaded");
    test_csv(filename, n1_csv_parse_threaded_avx256, "avx256 threaded");
   }
  return 0;
}
