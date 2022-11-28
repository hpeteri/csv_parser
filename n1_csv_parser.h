/*
  Copyright (c) 2022 Henrik Peteri.
  All rights reserved.
*/

#ifndef N1_CSV_PARSER_H
#define N1_CSV_PARSER_H

#define N1_CSV_STATIC_API static

// CSV parser for RFC 4180
// CSV spec can be found at : https://ww3w.ietf.org/rfc/rfc4180.txt

/***************************************
 Table of contents                      
----------------------------------------
* API
-- API struct declaration
-- API function declaration

* Implementation
-- Internal struct & enum definitions
-- Internal function declarations
-- Internal function definitions
-- API definitions
****************************************/

/* API struct declaration */

struct n1_CSV_Parser;
struct n1_CSV_Cell;

/* API function declaration */

N1_CSV_STATIC_API struct n1_CSV_Parser* n1_create_csv_parser(const char* filename);

N1_CSV_STATIC_API void n1_destroy_csv_parser(struct n1_CSV_Parser* parser);

//API for single-threaded parsing
N1_CSV_STATIC_API void n1_csv_parse_slow(struct n1_CSV_Parser* parser,
                                         char delim_token,
                                         char quote_token,
                                         char row_token);

//API for multi-threaded parsing
N1_CSV_STATIC_API void n1_csv_parse_threaded_slow(struct n1_CSV_Parser* parser,
                                                  char delim_token,
                                                  char quote_token,
                                                  char row_token);

N1_CSV_STATIC_API void n1_csv_parse_threaded_sse2(struct n1_CSV_Parser* parser,
                                                  char delim_token,
                                                  char quote_token,
                                                  char row_token);

N1_CSV_STATIC_API void n1_csv_parse_threaded_avx256(struct n1_CSV_Parser* parser,
                                                    char delim_token,
                                                    char quote_token,
                                                    char row_token);

/* IMPLEMENTATION */

//Before including this file, define N1_CSV_IMPLEMENTATION in one file to api definitions
#ifdef N1_CSV_IMPLEMENTATION

#define N1_CSV_TRUE  (1)
#define N1_CSV_FALSE (0)

#include <stdint.h>
#include <stdio.h>
#include <immintrin.h>

#if defined(__linux__)

#include <pthread.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <fcntl.h>

#elif defined(_WIN32)

#include <windows.h>
#include "Shlwapi.h"

#endif


#ifndef n1_csv_malloc
#include <stdlib.h>
#define n1_csv_malloc malloc
#endif

#ifndef n1_csv_free
#define n1_csv_free free
#endif

#ifndef n1_csv_realloc
#define n1_csv_realloc realloc
#endif

#ifndef n1_memset
#include <string.h>
#define n1_memset memset
#endif

/* INTERNAL STRUCT & ENUM DEFINITIONS */

typedef enum N1_CSV_TOKEN_TYPE{
  N1_CSV_TOKEN_TYPE_INVALID = 0,
  N1_CSV_TOKEN_TYPE_DELIM,
  N1_CSV_TOKEN_TYPE_QUOTE,
  N1_CSV_TOKEN_TYPE_ROW,
  N1_CSV_TOKEN_TYPE_NULL,
} N1_CSV_TOKEN_TYPE;

typedef struct n1_CSV_Cell{
  uint32_t start;
  uint32_t end;  
} n1_CSV_Cell;

typedef struct n1_CSV_Parser{
  char* filename;

  size_t file_size;

  uint32_t row_count;
  uint32_t column_count;
  uint64_t cell_count;

  n1_CSV_Cell* cell_data;
} n1_CSV_Parser;

typedef struct n1_CSV_Token{
  N1_CSV_TOKEN_TYPE type;
  uint32_t          offset;  
} n1_CSV_Token;

typedef struct n1_CSV_TokenStream{
  uint32_t      token_count;
  uint32_t      max_tokens;
  n1_CSV_Token* tokens;
} n1_CSV_TokenStream;

typedef struct n1_CSV_ParseInfo{
  n1_CSV_Parser*     parser;
  size_t             file_offset;
  size_t             bytes_to_read;
  n1_CSV_TokenStream tokens;
  char               delim_token, quote_token, row_token;
  void (*tokenize_proc)(n1_CSV_Parser*, n1_CSV_TokenStream*, char, char, char, char*, size_t, size_t);
 
} n1_CSV_ParseInfo;

/* INTERNAL FUNCTION DECLARAATIONS */

static void n1_csv_maybe_realloc_cell_data(n1_CSV_Parser* parser);

static void n1_csv_maybe_realloc_token_stream(n1_CSV_TokenStream* tokens);

static size_t n1_csv_get_page_size();

static uint32_t n1_csv_get_processor_count();

//threadproc for tokenizing section of a file.
static void n1_csv_tokenize_paged(n1_CSV_ParseInfo* parse_info);

static void n1_csv_tokenize_slow(n1_CSV_Parser* parser,
                                 n1_CSV_TokenStream* tokens,
                                 char delim_token,
                                 char quote_token,
                                 char row_token,
                                 char* file_buffer,
                                 size_t offset,
                                 size_t bytes_to_read);

static void n1_csv_tokenize_sse2(n1_CSV_Parser* parser,
                                 n1_CSV_TokenStream* tokens,
                                 char delim_token,
                                 char quote_token,
                                 char row_token,
                                 char* file_buffer,
                                 size_t offset,
                                 size_t bytes_to_read);

static void n1_csv_tokenize_avx256(n1_CSV_Parser* parser,
                                   n1_CSV_TokenStream* tokens,
                                   char delim_token,
                                   char quote_token,
                                   char row_token,
                                   char* file_buffer,
                                   size_t offset,
                                   size_t bytes_to_read);

//Convert tokens into cells.
static int8_t n1_csv_parse_tokens(n1_CSV_Parser* parser,
                                uint32_t token_count,
                                n1_CSV_Token* tokens,
                                n1_CSV_Token* prev_token,
                                int8_t* is_quoted,
                                uint32_t* cell_start,
                                uint32_t* row_idx,
                                int* start_quote_count,
                                int* end_quote_count,
                                int8_t* is_start_of_cell);

//Called from main API parse function with a tokenizer threadproc
//after file has been tokenized, n1_csv_parse_tokens is called.
static void n1_csv_parse_threaded(n1_CSV_Parser* parser,
                                  char delim_token,
                                  char quote_token,
                                  char row_token,
                                  void (*threadproc)(n1_CSV_Parser*, n1_CSV_TokenStream*, char, char, char, char*, size_t, size_t));


/* INTERNAL FUNCTION DEFINITIONS */

static void n1_csv_maybe_realloc_cell_data(n1_CSV_Parser* parser){
  
  if(parser->cell_count == (parser->column_count * parser->row_count)){
    parser->row_count <<= 1;
    parser->cell_data = (n1_CSV_Cell*)n1_csv_realloc(parser->cell_data, parser->column_count * parser->row_count * sizeof(n1_CSV_Cell));

    if(parser->cell_data == NULL){
      perror("realloc cell_data:");
    }
  }
}

static void n1_csv_maybe_realloc_token_stream(n1_CSV_TokenStream* tokens){
  
  if(tokens->token_count >= tokens->max_tokens){
    tokens->max_tokens <<= 1;
    tokens->tokens = (n1_CSV_Token*)n1_csv_realloc(tokens->tokens, tokens->max_tokens * sizeof(n1_CSV_Token));

    if(tokens->tokens == NULL){ //failed to realloc
      perror("realloc token stream: ");
    }    
  }
}

static size_t n1_csv_get_page_size(){

  static size_t page_size;

  if(!page_size){
#if defined(__linux__)

    page_size = sysconf(_SC_PAGESIZE);

#elif defined(_WIN32)

    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    page_size = system_info.dwPageSize;

#endif
  }

  return page_size;
}
static uint32_t n1_csv_get_processor_count(){
  static uint32_t processor_count;

  if(!processor_count){

#if defined(__linux__)
    processor_count = get_nprocs();    
#elif defined(_WIN32)
    processor_count = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
#endif
  }
  return processor_count;
}
static void n1_csv_tokenize_paged(n1_CSV_ParseInfo* parse_info){

  n1_CSV_Parser*      parser = parse_info->parser;
  n1_CSV_TokenStream* tokens = &parse_info->tokens;
  
  size_t offset    = parse_info->file_offset;
  size_t page_size = n1_csv_get_page_size();
    
  //fread fills buffer entirely, so allocate extra byte for null terminator
  char*  buffer    = (char*)n1_csv_malloc(page_size + 1);
  buffer[page_size] = 0;
  
  //open file
#if defined(__linux__)
  int file = open(parser->filename,
                  O_RDONLY);
  if(file == -1){
    perror("Failed to reopen file:");
    //abort?
  }

#elif defined(_WIN32)
  HANDLE file = CreateFile(parser->filename,
                           GENERIC_READ,
                           FILE_SHARE_READ,
                           NULL,
                           OPEN_EXISTING,
                           FILE_ATTRIBUTE_READONLY,
                           NULL);
  
  if(file == INVALID_HANDLE_VALUE){
    perror("Failed to reopen file:");
    //abort?
  }

#endif
  
#if defined(__linux__)
  //set file position
  //fseek(file, (int32_t)offset, SEEK_SET);
  lseek(file, offset, SEEK_SET);
#elif defined(_WIN32)
  SetFilePointer(file, (LONG)offset, ((LONG*)&offset) + 1, FILE_BEGIN);
#endif
  
  
  const int iterations = (int)(parse_info->bytes_to_read / page_size);
        
  for(int i = 0; i < iterations; i++){
#if defined(__linux__)
    
    read(file, buffer, page_size);
    
#elif defined(_WIN32)
    
    ReadFile(file,
             buffer,
             (DWORD)page_size,
             NULL,
             NULL);
    
#endif
    
    parse_info->tokenize_proc(parser,
                              tokens,
                              parse_info->delim_token,
                              parse_info->quote_token,
                              parse_info->row_token,
                              buffer,
                              offset,
                              page_size); 
    offset += page_size;
  }
  
  {
#if defined(__linux__)
    read(file, buffer, page_size);

#elif defined(_WIN32)
    
    ReadFile(file,
             buffer,
             (DWORD)page_size,
             NULL,
             NULL);    

#endif
    
    buffer[parse_info->bytes_to_read % page_size] = 0;
    parse_info->tokenize_proc(parser,
                              tokens,
                              parse_info->delim_token,
                              parse_info->quote_token,
                              parse_info->row_token,
                              buffer,
                              offset,
                              parse_info->bytes_to_read % page_size);
  }
  n1_csv_free(buffer);

#if defined(__linux__)
  
  close(file);
  
#elif defined(_WIN32)
  
  CloseHandle(file);
  
#endif
}

static void n1_csv_tokenize_slow(n1_CSV_Parser* parser,
                                 n1_CSV_TokenStream* tokens,
                                 char delim_token,
                                 char quote_token,
                                 char row_token,
                                 char* file_buffer,
                                 size_t offset,
                                 size_t bytes_to_read){
  char*       at  = file_buffer;
  const char* end = at + bytes_to_read;
  
  for(; at < end; at++){
    const int8_t has_delim = *at == delim_token;
    const int8_t has_quote = *at == quote_token;
    const int8_t has_row   = *at == row_token;
    const int8_t has_null  = *at == 0;
    const int8_t has_token = has_delim || has_quote || has_row || has_null;
        
    if(!has_token){
      continue;
    }
    
    n1_CSV_Token token;
      
    if(has_null){
      token.type = N1_CSV_TOKEN_TYPE_NULL;  
    }else if(has_quote){
      token.type = N1_CSV_TOKEN_TYPE_QUOTE;
    }else if(has_delim){
      token.type = N1_CSV_TOKEN_TYPE_DELIM;
    }else if(has_row){
      token.type = N1_CSV_TOKEN_TYPE_ROW;
    }
      
    token.offset = (uint32_t)(at - file_buffer + offset);
    tokens->tokens[tokens->token_count++] = token;
      
    n1_csv_maybe_realloc_token_stream(tokens);
      
    if(has_null){ return; }
    
  }
}

static void n1_csv_tokenize_sse2(n1_CSV_Parser* parser,
                                 n1_CSV_TokenStream* tokens,
                                 char delim_token,
                                 char quote_token,
                                 char row_token,
                                 char* file_buffer,
                                 size_t offset,
                                 size_t bytes_to_read){
    
  const __m128i delim    = _mm_set1_epi8(delim_token);
  const __m128i quote    = _mm_set1_epi8(quote_token);
  const __m128i row_sep  = _mm_set1_epi8(row_token);
  const __m128i nullchar = _mm_set1_epi8(0);

  const __m128i one = _mm_set1_epi8(1);
  const __m128i zero = _mm_setzero_si128();

  __m128i*       at  = (__m128i*)(file_buffer);
  const __m128i* end = (__m128i*)((char*)at + bytes_to_read);
  
  for(; at < end; at++){
    __m128i it;
    memcpy(&it, at, sizeof(it));
    
    const __m128i has_delim = _mm_cmpeq_epi8(it, delim);
    const __m128i has_quote = _mm_cmpeq_epi8(it, quote);
    const __m128i has_row   = _mm_cmpeq_epi8(it, row_sep);
    const __m128i has_null  = _mm_cmpeq_epi8(it, nullchar);
    const __m128i has_token = _mm_or_si128(_mm_or_si128(_mm_or_si128(has_delim, has_quote), has_row), has_null);

    
    const __m128i tmp = _mm_sad_epu8(_mm_and_si128(has_token, one), zero);
    int token_count = ((uint8_t*)&tmp)[0] + ((uint8_t*)&tmp)[8];
    
    if(!token_count){
      continue;
    }
    
    char* file_at = (char*)at;
    
    for(int i = 0; i < 16 && token_count; i++, file_at++){
      
      n1_CSV_Token token;
      
      if(*file_at == 0){
        token.type = N1_CSV_TOKEN_TYPE_NULL;  
      }else if(*file_at == quote_token){
        token.type = N1_CSV_TOKEN_TYPE_QUOTE;
        token_count --;
      }else if(*file_at == delim_token){
        token.type = N1_CSV_TOKEN_TYPE_DELIM;
        token_count --;
      }else if(*file_at == row_token){
        token.type = N1_CSV_TOKEN_TYPE_ROW;
        token_count --;
      }else{
        continue;
      }
      
      token.offset = (uint32_t)(file_at - file_buffer + offset);
      tokens->tokens[tokens->token_count++] = token;
      
      n1_csv_maybe_realloc_token_stream(tokens);
      
      if(*file_at == 0){ return; }
      
    }
  }
}

static void n1_csv_tokenize_avx256(n1_CSV_Parser* parser,
                                   n1_CSV_TokenStream* tokens,
                                   char delim_token,
                                   char quote_token,
                                   char row_token,
                                   char* file_buffer,
                                   size_t offset,
                                   size_t bytes_to_read){

  const __m256i delim    = _mm256_set1_epi8(delim_token);
  const __m256i quote    = _mm256_set1_epi8(quote_token);
  const __m256i row_sep  = _mm256_set1_epi8(row_token);
  const __m256i nullchar = _mm256_set1_epi8(0);

  const __m256i one  = _mm256_set1_epi8(1);
  const __m256i zero = _mm256_setzero_si256();

  __m256i* at  = (__m256i*)(file_buffer);
  const __m256i* end = (__m256i*)((char*)at + bytes_to_read);
  
  for(; at < end; at++){
    __m256i it;
    memcpy(&it, at, sizeof(it));
    
    const __m256i has_delim = _mm256_cmpeq_epi8(it, delim);
    const __m256i has_quote = _mm256_cmpeq_epi8(it, quote);
    const __m256i has_row   = _mm256_cmpeq_epi8(it, row_sep);
    const __m256i has_null  = _mm256_cmpeq_epi8(it, nullchar);
    const __m256i has_token = _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(has_delim, has_quote), has_row), has_null);

    const __m256i tmp = _mm256_sad_epu8(_mm256_and_si256(has_token, one), zero);
    int token_count = ((uint8_t*)&tmp)[0] + ((uint8_t*)&tmp)[8] + ((uint8_t*)&tmp)[16] + ((uint8_t*)&tmp)[24];

    if(!token_count){
      continue;
    }
    char* file_at = (char*)at;
    
    for(int i = 0; i < 32 && token_count; i++, file_at++){
        
      n1_CSV_Token token;
        
      if(*file_at == 0){
        token.type = N1_CSV_TOKEN_TYPE_NULL;  
      }else if(*file_at == quote_token){
        token.type = N1_CSV_TOKEN_TYPE_QUOTE;
        token_count --;
      }else if(*file_at == delim_token){
        token.type = N1_CSV_TOKEN_TYPE_DELIM;
        token_count --;
      }else if(*file_at == row_token){
        token.type = N1_CSV_TOKEN_TYPE_ROW;
        token_count --;
      }else{
        continue;
      }
        
      token.offset = (uint32_t)(file_at - file_buffer + offset);
      tokens->tokens[tokens->token_count++] = token;
        
      n1_csv_maybe_realloc_token_stream(tokens);

      if(*file_at == 0){ return; }
    }
  }
}

static int8_t n1_csv_parse_tokens(n1_CSV_Parser* parser,
                                uint32_t token_count,
                                n1_CSV_Token* tokens,
                                n1_CSV_Token* prev_token,
                                int8_t* is_quoted,
                                uint32_t* cell_start,
                                uint32_t* row_idx,
                                int* start_quote_count,
                                int* end_quote_count,
                                int8_t* is_start_of_cell){

  for(uint32_t token_idx = 0; token_idx < token_count; token_idx++){
    
    n1_CSV_Token token = tokens[token_idx];
    
    //is token next to previous token
    int8_t next_to_previous = (prev_token->offset + 1) == token.offset;

    if(!next_to_previous){
      *is_start_of_cell = N1_CSV_FALSE;
    }
      
    //handle quotation at the start of cell
    if(token.type == N1_CSV_TOKEN_TYPE_QUOTE){
      if(*is_start_of_cell){ //if start of cell, keep calculating how many quotes we have
        if(next_to_previous){
          (*start_quote_count) ++;
        }
      }else{
        if(!next_to_previous || prev_token->type != N1_CSV_TOKEN_TYPE_QUOTE){
          (*end_quote_count) = 0;
        }
        (*end_quote_count) ++;
      }
    }else{
      *is_start_of_cell = N1_CSV_FALSE;
      *is_quoted = *start_quote_count % 2;
      if(prev_token->type == N1_CSV_TOKEN_TYPE_QUOTE && *is_quoted){
        if(*end_quote_count){
          *is_quoted = (*end_quote_count % 2) == 0;
        }
        *end_quote_count = 0;
      }
      
      if(token.type == N1_CSV_TOKEN_TYPE_NULL){
        n1_CSV_Cell cell = {
          *cell_start,
          token.offset
        };
          
        parser->cell_data[parser->cell_count++] = cell;
        
        n1_csv_maybe_realloc_cell_data(parser);
        *prev_token = token;
  
        return N1_CSV_FALSE;
      }
        
      else if(token.type == N1_CSV_TOKEN_TYPE_DELIM ||
              token.type == N1_CSV_TOKEN_TYPE_ROW){
          
        if(!*is_quoted){
          
          n1_CSV_Cell cell = {
            *cell_start,
            token.offset
          };
          
          parser->cell_data[parser->cell_count++] = cell;
          n1_csv_maybe_realloc_cell_data(parser);
            
          *is_start_of_cell = N1_CSV_TRUE;
          *start_quote_count = 0;
          *cell_start = token.offset + 1;
          
          if(token.type == N1_CSV_TOKEN_TYPE_ROW){
            //set actual column count after processing the first line
            if(!*row_idx){ 
              uint32_t original_column_count = parser->column_count;
              parser->column_count = (uint32_t)parser->cell_count;
              parser->row_count    = original_column_count / parser->column_count;
            }
            (*row_idx) ++;
          }
        }
      }        
    }      
    *prev_token = token;
  }
  return N1_CSV_TRUE;
}

static void n1_csv_parse_threaded(n1_CSV_Parser* parser,
                                  char delim_token,
                                  char quote_token,
                                  char row_token,
                                  void (*threadproc)(n1_CSV_Parser*, n1_CSV_TokenStream*, char, char, char, char*, size_t, size_t)){

  if(!parser->file_size){
    return;
  }

  const size_t   page_size    = n1_csv_get_page_size();
  const uint32_t processor_count = n1_csv_get_processor_count();
  
  uint32_t thread_count = (uint32_t)(parser->file_size / page_size);
  if(!thread_count){
    thread_count = 1;
  }else if(thread_count > processor_count){
    thread_count = processor_count;
  }
    
  size_t bytes_to_read = (parser->file_size / thread_count);
  bytes_to_read += 32 - (bytes_to_read % 32);
  
  size_t offset = 0;

#if defined(__linux__)
  pthread_t* threads = (pthread_t*)n1_csv_malloc(sizeof(pthread_t) * thread_count);
#elif defined(_WIN32)
  HANDLE* threads = (HANDLE*)n1_csv_malloc(sizeof(HANDLE) * thread_count);
#endif
  
  n1_CSV_ParseInfo* infos = (n1_CSV_ParseInfo*)n1_csv_malloc(sizeof(n1_CSV_ParseInfo) * thread_count);
  
  for(uint32_t i = 0; i < thread_count; i++){
    n1_CSV_ParseInfo* info   = &infos[i];
    info->parser             = parser;
    info->file_offset        = offset;
    info->bytes_to_read     = bytes_to_read;

    if(info->bytes_to_read + offset > parser->file_size){
      info->bytes_to_read = parser->file_size - offset;
    }

    info->delim_token        = delim_token;
    info->quote_token        = quote_token;
    info->row_token          = row_token;
    info->tokens.token_count = 0;
    info->tokens.max_tokens  = 64;
    info->tokens.tokens      = (n1_CSV_Token*)n1_csv_malloc(info->tokens.max_tokens * sizeof(n1_CSV_Token));
    info->tokenize_proc      = threadproc;
    
    offset += bytes_to_read;
        
#if defined(__linux__)
    pthread_create(&threads[i], NULL, (void*(*)(void*))n1_csv_tokenize_paged, &infos[i]);
#elif defined(_WIN32)
    DWORD id;
    threads[i] = CreateThread(NULL, 0, (DWORD(*)(void*))n1_csv_tokenize_paged, &infos[i], 0, &id);
#endif
  }
  
  parser->column_count  = 256;
  parser->row_count     = 1;
  parser->cell_data     = (n1_CSV_Cell*)n1_csv_malloc(sizeof(n1_CSV_Cell) * parser->column_count);
  
  //--------------------------
  
  n1_CSV_Token prev_token        = {0, 0};
  int8_t       is_quoted         = N1_CSV_FALSE;
  uint32_t     cell_start        = 0;
  uint32_t     row_idx           = 0;
  int8_t       keep_processing   = N1_CSV_TRUE;
  int          start_quote_count = 0;
  int          end_quote_count   = 0;
  int8_t       is_start_of_cell  = N1_CSV_TRUE;

  int8_t run = N1_CSV_TRUE;
  for(uint32_t i = 0; i < thread_count; i++){
    
#if defined(__linux__)
    pthread_join(threads[i], NULL);
#elif defined(_WIN32)
    WaitForSingleObject(threads[i], INFINITE);
#endif
    if(run)
      run = n1_csv_parse_tokens(parser,
                                infos[i].tokens.token_count,
                                infos[i].tokens.tokens,
                                &prev_token,
                                &is_quoted,
                                &cell_start,
                                &row_idx,
                                &start_quote_count,
                                &end_quote_count,
                                &is_start_of_cell);
    n1_csv_free(infos[i].tokens.tokens);
  }
  
  n1_csv_free(threads);
  n1_csv_free(infos);
}

/* API DEFINITIONS */

N1_CSV_STATIC_API struct n1_CSV_Parser* n1_create_csv_parser(const char* filename){

  n1_CSV_Parser* parser = (n1_CSV_Parser*)n1_csv_malloc(sizeof(n1_CSV_Parser));
  n1_memset(parser, 0, sizeof(*parser));

  //string copy file name
  size_t len       = strlen(filename);
  parser->filename = (char*)n1_csv_malloc(len + 1);
  memcpy(parser->filename, filename, len + 1);

#if defined(__linux__)

  struct stat file_stat;

  if(!stat(filename, &file_stat)){
    parser->file_size = file_stat.st_size;
    parser->file_size += 32 - (parser->file_size % 32);
  }else{
    perror("Failed to open file:");
  }
  
#elif defined(_WIN32)
  
  HANDLE file = CreateFile(filename,
                           GENERIC_READ,
                           FILE_SHARE_READ,
                           NULL,
                           OPEN_EXISTING,
                           FILE_ATTRIBUTE_READONLY,
                           NULL);
 
  if(file != INVALID_HANDLE_VALUE){
    LARGE_INTEGER file_size;
    GetFileSizeEx(file, &file_size);
    parser->file_size = file_size.QuadPart;
    parser->file_size += 32 - (parser->file_size % 32);

    CloseHandle(file);
  }else{
    perror("Failed to open file:");
  }
  
#endif
  return parser;
}

N1_CSV_STATIC_API void n1_destroy_csv_parser(n1_CSV_Parser* parser){

  n1_csv_free(parser->cell_data);
  n1_csv_free(parser->filename);

  n1_csv_free(parser);
  
}

N1_CSV_STATIC_API void n1_csv_parse_slow(n1_CSV_Parser* parser,
                                         char delim_token,
                                         char quote_token,
                                         char row_token){

  if(!parser->file_size){
    return;
  }
  
  n1_CSV_ParseInfo info;
  info.parser             = parser;
  info.file_offset        = 0;
  info.bytes_to_read      = parser->file_size;
  info.delim_token        = delim_token;
  info.quote_token        = quote_token;
  info.row_token          = row_token;
  info.tokens.token_count = 0;
  info.tokens.max_tokens  = 64;
  info.tokens.tokens      = (n1_CSV_Token*)n1_csv_malloc(info.tokens.max_tokens * sizeof(n1_CSV_Token));
  info.tokenize_proc      = n1_csv_tokenize_slow;

  n1_csv_tokenize_paged(&info);
  
  parser->column_count  = 256;
  parser->row_count     = 1;
  parser->cell_data     = (n1_CSV_Cell*)n1_csv_malloc(sizeof(n1_CSV_Cell) * parser->column_count);
  
  n1_CSV_Token prev_token        = {0,0};
  int8_t       is_quoted         = N1_CSV_FALSE;
  uint32_t     cell_start        = 0;
  uint32_t     row_idx           = 0;
  int8_t       keep_processing   = N1_CSV_TRUE;
  int          start_quote_count = 0;
  int          end_quote_count   = 0;
  int8_t       is_start_of_cell  = N1_CSV_TRUE;
  
  n1_csv_parse_tokens(parser,
                      info.tokens.token_count,
                      info.tokens.tokens,
                      &prev_token,
                      &is_quoted,
                      &cell_start,
                      &row_idx,
                      &start_quote_count,
                      &end_quote_count,
                      &is_start_of_cell);
  
  n1_csv_free(info.tokens.tokens);
}


N1_CSV_STATIC_API void n1_csv_parse_threaded_slow(n1_CSV_Parser* parser,
                                                  char delim_token,
                                                  char quote_token,
                                                  char row_token){
  n1_csv_parse_threaded(parser,
                        delim_token,
                        quote_token,
                        row_token,
                        n1_csv_tokenize_slow);
}

N1_CSV_STATIC_API void n1_csv_parse_threaded_sse2(n1_CSV_Parser* parser,
                                                  char delim_token,
                                                  char quote_token,
                                                  char row_token){
  n1_csv_parse_threaded(parser,
                        delim_token,
                        quote_token,
                        row_token,
                        n1_csv_tokenize_sse2);
}

N1_CSV_STATIC_API void n1_csv_parse_threaded_avx256(n1_CSV_Parser* parser,
                                                    char delim_token,
                                                    char quote_token,
                                                    char row_token){
  n1_csv_parse_threaded(parser,
                        delim_token,
                        quote_token,
                        row_token,
                        n1_csv_tokenize_avx256);
}

#endif
#endif
