#ifndef N1_CSV_PARSER_H
#define N1_CSV_PARSER_H

#define N1_CSV_STATIC_API static

#include <stdint.h>
#include <cstdio>

//structs
struct n1_CSV_Parser;

//functions
N1_CSV_STATIC_API struct n1_CSV_Parser* n1_create_csv_parser(const char* filename);
N1_CSV_STATIC_API void n1_destroy_csv_parser(n1_CSV_Parser* parser);


#ifdef N1_CSV_IMPLEMENTATION

#ifndef n1_malloc
#include <cstdlib>
#define n1_malloc malloc
#endif

#ifndef n1_free
#define n1_free free
#endif

#ifndef n1_memset
#include <cstring>
#define n1_memset memset
#endif

struct n1_CSV_Parser{
  char*  file_buffer;
  size_t file_size;
};

N1_CSV_STATIC_API struct n1_CSV_Parser* n1_create_csv_parser(const char* filename){
  n1_CSV_Parser* parser = (n1_CSV_Parser*)n1_malloc(sizeof(n1_CSV_Parser));
  n1_memset(parser, 0, sizeof(*parser));

  FILE* file = fopen(filename, "r");
  if(file != NULL){
    fseek(file, 0, SEEK_END);
    parser->file_size = ftell(file);
    parser->file_buffer = (char*)n1_malloc(parser->file_size);

    rewind(file);

    fread(parser->file_buffer,
          parser->file_size,
          1,
          file);

    printf("%s\n", parser->file_buffer);
    fclose(file);
  }
  return parser;
}
N1_CSV_STATIC_API void n1_destroy_csv_parser(n1_CSV_Parser* parser){

  n1_free(parser->file_buffer);
  n1_free(parser);
}

#endif
#endif
