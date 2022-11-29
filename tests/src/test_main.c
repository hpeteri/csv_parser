#define N1_CSV_IMPLEMENTATION
#include "n1_csv_parser.h"

#define N1_TIMESTAMP_IMPLEMENTATION
#include "timestamp/n1_timestamp.h"

#define PRINT_LOG_TABLE_HEADER(){printf("File | Size | Columns | Rows | Cells | Info | Time (ms) | MBps \n---|---|---|---|---|---|---|---\n");} 

#if defined(__linux__)

#define PRINT_LOG_PARSER(name, parser, info, time) {printf("%s | %.4f MB | %u |  %u | %ld | '%s' |  %f | %f \n", name, (parser->file_size / 1024.0 / 1024.0), parser->column_count, parser->row_count, parser->cell_count, info, (double)time / 1000.0, (double)(parser->file_size / 1024.0 / 1024.0) / (time / 1000000.0));}

#elif defined(_WIN32)

#define PRINT_LOG_PARSER(name, parser, info, time) \
  {printf("%s | %.4f MB | %u |  %u | %lld | '%s' |  %f | %f \n",       \
          name,                                                         \
          parser->file_size / (1024.0 *1024.0),                         \
          parser->column_count,                                         \
          parser->row_count,                                            \
          parser->cell_count,                                           \
          info,                                                         \
          (double)time / (double)1000.0f,                               \
          ((double)parser->file_size / (double)1024.0 / (double)1024.0) / (time / 1000000.0));}

#endif

void test_csv(const char* filename, void (*parsefunc)(struct n1_CSV_Parser* parser, char delim, char quote, char newline), const char* info){

  const int iter = 1;
  for(int i = 0; i < iter; i++){
    uint64_t start = n1_gettimestamp_microseconds();
    struct n1_CSV_Parser* parser = n1_create_csv_parser(filename);
    if(!parser->file_size){
      n1_destroy_csv_parser(parser);
      return;
    }
      
    parsefunc(parser, ',', '"', '\n');
    
    uint64_t end = n1_gettimestamp_microseconds();
    uint64_t time = (end - start);
    
    PRINT_LOG_PARSER(filename, parser, info, time);

    uint64_t start_0 = n1_gettimestamp_microseconds();
    for(uint32_t i = 0; i < parser->row_count; i++){
      for(uint32_t x = 0; x < parser->column_count; x++){
        n1_CSV_String s = n1_csv_get_cell_transient(parser, x, i);
        
        if(s.data){
          //printf("(%.*s), ", s.length, s.data);
        }else{
          i = -2;
        }
      }
      //printf("\n");
    }
    
    uint64_t end_0 = n1_gettimestamp_microseconds();
    uint64_t time_0 = (end_0 - start_0);
    printf("get cells took %f ms (%f MBps)\n", (time_0) / 1000.0f,
           (double)(parser->file_size / 1024.0 / 1024.0) / (time_0 / 1000000.0));
    n1_destroy_csv_parser(parser);
  }
}

int main(){
  const char* filenames[] = {

    "test_data/test.csv",
    
    
    "test_data/denver_crime_data/test_data/offense_codes.csv",        
    
    "test_data/denver_crime_data/test_data/crime.csv", 
    
    "test_data/airbnb_paris/test_data/calendar.csv", 
    "test_data/airbnb_paris/test_data/listings.csv", 
    "test_data/airbnb_paris/test_data/neighbourhoods.csv", 
    "test_data/airbnb_paris/test_data/reviews.csv", 

    "test_data/used_cars/test_data/vehicles.csv", 
    "test_data/sha1_dump/test_data/pwnd.csv", 
#if 1

    "test_data/ddos/test_data/unbalaced_20_80_dataset.csv", 
    "test_data/ddos/test_data/final_dataset.csv" 

#endif
  };
  
  PRINT_LOG_TABLE_HEADER();
  for(size_t i = 0; i < sizeof(filenames) / sizeof(*filenames); i++){
    test_csv(filenames[i], n1_csv_parse_slow, "slow");
    test_csv(filenames[i], n1_csv_parse_threaded_slow, "slow threaded");
    test_csv(filenames[i], n1_csv_parse_threaded_sse2, "sse2 threaded");
    test_csv(filenames[i], n1_csv_parse_threaded_avx256, "avx256 threaded");
  }
  printf("done\n");
  return 0;
}
