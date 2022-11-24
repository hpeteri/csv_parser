#ifndef N1_CSV_PARSER_H
#define N1_CSV_PARSER_H

#define N1_CSV_STATIC_API static

#include <stdint.h>
#include <cstdio>
#include <immintrin.h>



// CSV parser for RFC 4180
// CSV spec can be found at : https://ww3w.ietf.org/rfc/rfc4180.txt
  
struct n1_CSV_Parser;
struct n1_CSV_Cell;

N1_CSV_STATIC_API struct n1_CSV_Parser* n1_create_csv_parser(const char* filename);
N1_CSV_STATIC_API void n1_destroy_csv_parser(n1_CSV_Parser* parser);

N1_CSV_STATIC_API void n1_csv_parse_slow(n1_CSV_Parser* parser, char delim_token, char quote_token, char row_token);
N1_CSV_STATIC_API void n1_csv_parse_threaded_slow(n1_CSV_Parser* parser, char delim_token, char quote_token, char row_token);
N1_CSV_STATIC_API void n1_csv_parse_threaded_sse2(n1_CSV_Parser* parser, char delim_token, char quote_token, char row_token);
N1_CSV_STATIC_API void n1_csv_parse_threaded_avx256(n1_CSV_Parser* parser, char delim_token, char quote_token, char row_token);

#ifdef N1_CSV_IMPLEMENTATION


#if defined(_WIN32)
#include "Shlwapi.h"
#endif


#ifndef n1_csv_malloc
#include <cstdlib>
#define n1_csv_malloc malloc
#endif

#ifndef n1_csv_free
#define n1_csv_free free
#endif

#ifndef n1_csv_realloc
#define n1_csv_realloc realloc
#endif

#ifndef n1_memset
#include <cstring>
#define n1_memset memset
#endif

#include <emmintrin.h>

struct n1_CSV_Cell{
  uint32_t start;
  uint32_t end;  
};

struct n1_CSV_Parser{
  char* filename;
  
  //FILE*  file_handle;
  //char*  file_buffer;
  size_t file_size;

  uint32_t row_count;
  uint32_t column_count;

  uint64_t cell_count;

  n1_CSV_Cell* cell_data;
};
enum N1_CSV_TOKEN_TYPE{
  N1_CSV_TOKEN_TYPE_INVALID = 0,
  N1_CSV_TOKEN_TYPE_DELIM,
  N1_CSV_TOKEN_TYPE_QUOTE,
  N1_CSV_TOKEN_TYPE_ROW,
  N1_CSV_TOKEN_TYPE_NULL,
};
const char* token_names[] = {"invalid", "delim", "quote", "row", "null"};
struct n1_CSV_Token{
  N1_CSV_TOKEN_TYPE type;
  uint32_t          offset;  
};

struct n1_CSV_TokenStream{
  uint32_t          token_count;
  uint32_t          max_tokens;
  n1_CSV_Token*     tokens;
};

struct n1_CSV_ParseInfo{
  n1_CSV_Parser*     parser;
  size_t             file_offset;
  size_t             bytes_to_read;
  n1_CSV_TokenStream tokens;
  char      delim_token, quote_token, row_token;

  void (*tokenize_proc)(n1_CSV_Parser*, n1_CSV_TokenStream*, char, char, char, char*, size_t, size_t);
};
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



static bool n1_csv_parse_tokens_internal(n1_CSV_Parser* parser,
                                         uint32_t token_count,
                                         n1_CSV_Token* tokens,
                                         n1_CSV_Token* prev_token,
                                         bool* is_quoted,
                                         uint32_t* cell_start,
                                         uint32_t* row_idx,
                                         int* start_quote_count,
                                         int* end_quote_count,
                                         bool* start_of_cell);

N1_CSV_STATIC_API struct n1_CSV_Parser* n1_create_csv_parser(const char* filename){
  n1_CSV_Parser* parser = (n1_CSV_Parser*)n1_csv_malloc(sizeof(n1_CSV_Parser));
  n1_memset(parser, 0, sizeof(*parser));
  
  size_t len = strlen(filename);
  parser->filename = (char*)n1_csv_malloc(len + 1);
  memcpy(parser->filename, filename, len + 1);

#if defined(__linux__)
  FILE* file = fopen(filename, "r");
#elif defined(_WIN32)
  HANDLE file = CreateFile(filename,
                           GENERIC_READ,
                           FILE_SHARE_READ,
                           NULL,
                           OPEN_EXISTING,
                           FILE_ATTRIBUTE_READONLY,
                           NULL);
#endif


  
  if(file != NULL){
#if defined(__linux__)
    fseek(file, 0, SEEK_END);

    //align filesize to avx256
    parser->file_size = ftell(file);

    //Failed to get size?
    if(parser->file_size == (size_t)-1){
      perror("ftell(): ");
      parser->file_size = 0;
    }else{
      parser->file_size += 32 - (parser->file_size % 32);
    }
    fclose(file);

#elif defined(_WIN32)
    LARGE_INTEGER file_size;
    GetFileSizeEx(file, &file_size);
    parser->file_size = file_size.QuadPart;
    parser->file_size += 32 - (parser->file_size % 32);

    CloseHandle(file);
#endif
  }else{
    perror("Failed to open file:");
  }
  return parser;
}

N1_CSV_STATIC_API void n1_destroy_csv_parser(n1_CSV_Parser* parser){

  n1_csv_free(parser->cell_data);
  //n1_csv_free(parser->file_buffer);

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
  
  n1_CSV_Token prev_token        = {};
  bool         is_quoted         = false;
  uint32_t     cell_start        = 0;
  uint32_t     row_idx           = 0;
  bool         keep_processing   = true;
  int          start_quote_count = 0;
  int          end_quote_count   = 0;
  bool         start_of_cell     = true;
  
  n1_csv_parse_tokens_internal(parser,
                               info.tokens.token_count,
                               info.tokens.tokens,
                               &prev_token,
                               &is_quoted,
                               &cell_start,
                               &row_idx,
                               &start_quote_count,
                               &end_quote_count,
                               &start_of_cell);
  
  n1_csv_free(info.tokens.tokens);
}


#if defined(__linux__)
#include <pthread.h>
#include <unistd.h>
static size_t n1_csv_internal_get_page_file_size(){
  static size_t page_file_size = 0;
  
  if(!page_file_size){
    page_file_size = sysconf(_SC_PAGESIZE);
    printf("page file size got : %ld\n", page_file_size);
  }
  return page_file_size;
}
#elif defined(_WIN32)
#include <windows.h>
#endif

static char* n1_csv_internal_read_file(n1_CSV_Parser* parser,
                                       size_t offset,
                                       size_t bytes){
#if defined(__linux__)
  FILE* file = fopen(parser->filename, "r");
#elif defined(_WIN32)
  FILE* file;
  fopen_s(&file, parser->filename, "r");
#endif

  if(!file){
    perror("Failed to reopen file:");
  }

  char* buffer = (char*)n1_csv_malloc(bytes);
  if(!buffer){
    perror("csv_internal_read_file malloc:");
    return buffer;
  }
  fseek(file, (int32_t)offset, SEEK_SET);
  
  fread(buffer,
        bytes,
        1,
        file);  
  fclose(file);
  return buffer;  
}
static void ASSERT_BUFFERS_MATCH(char* buffer_0,
                                 char* buffer_1,
                                 size_t size){

  for(size_t i = 0; i < size; i++){
    if(buffer_0[i] != buffer_1[i]){
#if defined(__linux)
      printf("File buffers do not match at %ld / %ld\n", i, size);
#elif defined(_WIN32)
      printf("File buffers do not match at %lld / %lld\n", i, size);
#endif
      *(int*)nullptr = 0;
    }
  }
}

static void n1_csv_internal_maybe_realloc_cell_data(n1_CSV_Parser* parser){
  if(parser->cell_count == (parser->column_count * parser->row_count)){
    parser->row_count <<= 1;
    parser->cell_data = (n1_CSV_Cell*)n1_csv_realloc(parser->cell_data, parser->column_count * parser->row_count * sizeof(n1_CSV_Cell));

    if(parser->cell_data == NULL){
      perror("realloc cell_data:");
    }
  }
}
static void n1_csv_internal_maybe_realloc_token_stream(n1_CSV_TokenStream* tokens){
  
  if(tokens->token_count >= tokens->max_tokens){
    tokens->max_tokens <<= 1;
    tokens->tokens = (n1_CSV_Token*)n1_csv_realloc(tokens->tokens, tokens->max_tokens * sizeof(n1_CSV_Token));

    if(tokens->tokens == NULL){ //failed to realloc
      perror("realloc token stream: ");
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

    if(token_count){

      char* file_at = (char*)at;
      
      for(int i = 0; i < 32 && token_count; i++, file_at++){
        
        n1_CSV_Token token;
        
        if(*file_at == '\0'){
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
        
        n1_csv_internal_maybe_realloc_token_stream(tokens);

        if(*file_at == '\0'){
          return;
        }
      }
    }
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
    
    if(token_count){

      char* file_at = (char*)at;
      
      for(int i = 0; i < 16 && token_count; i++, file_at++){
        
        n1_CSV_Token token;
        
        if(*file_at == '\0'){
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
        
        n1_csv_internal_maybe_realloc_token_stream(tokens);
        
        if(*file_at == '\0'){
          return;
        }
      }
    }
  }
}

static void n1_csv_tokenize_paged(n1_CSV_ParseInfo* parse_info){

  n1_CSV_Parser*      parser = parse_info->parser;
  n1_CSV_TokenStream* tokens = &parse_info->tokens;
  
  size_t offset    = parse_info->file_offset;
  size_t page_size = 1024L * 1024L * 32;
    
  //fread fills buffer entirely, so allocate extra byte for null terminator
  char*  buffer    = (char*)n1_csv_malloc(page_size + 1);
  buffer[page_size] = 0;
  
  //open file
#if defined(__linux__)
  FILE* file = fopen(parser->filename, "r");
#elif defined(_WIN32)
  FILE* file;
  fopen_s(&file, parser->filename, "r");
#endif
  
  if(!file){
    perror("Failed to reopen file:");
    //abort?
  }
  
  fseek(file, (int32_t)offset, SEEK_SET);
  
  
  const int iterations = (int)(parse_info->bytes_to_read / page_size);
        
  for(int i = 0; i < iterations; i++){
    fread(buffer,
          page_size,
          1,
          file);  
    
    offset += page_size;
    
    parse_info->tokenize_proc(parser,
                              tokens,
                              parse_info->delim_token,
                              parse_info->quote_token,
                              parse_info->row_token,
                              buffer,
                              offset,
                              page_size); 
  }
  
  {
    fread(buffer,
          parse_info->bytes_to_read % page_size,
          1,
          file);
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
  fclose(file);
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
    const bool has_delim = *at == delim_token;
    const bool has_quote = *at == quote_token;
    const bool has_row   = *at == row_token;
    const bool has_null  = *at == 0;
    const bool has_token = has_delim || has_quote || has_row || has_null;
        
    if(has_token){
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
      
      n1_csv_internal_maybe_realloc_token_stream(tokens);

      if(has_null){
        return;
      }
    }
  }
}

static bool n1_csv_parse_tokens_internal(n1_CSV_Parser* parser,
                                         uint32_t token_count,
                                         n1_CSV_Token* tokens,
                                         n1_CSV_Token* prev_token,
                                         bool* is_quoted,
                                         uint32_t* cell_start,
                                         uint32_t* row_idx,
                                         int* start_quote_count,
                                         int* end_quote_count,
                                         bool* start_of_cell){
  for(uint32_t token_idx = 0; token_idx < token_count; token_idx++){
    
    n1_CSV_Token token = tokens[token_idx];
    
    //is token next to previous token
    bool next_to_previous = (prev_token->offset + 1) == token.offset;

    if(!next_to_previous){
      *start_of_cell = false;
    }
      
    //handle quotation at the start of cell
    if(token.type == N1_CSV_TOKEN_TYPE_QUOTE){
      if(*start_of_cell){ //if start of cell, keep calculating how many quotes we have
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
      *start_of_cell = false;
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
        
        n1_csv_internal_maybe_realloc_cell_data(parser);
        *prev_token = token;
  
        return false;
      }
        
      else if(token.type == N1_CSV_TOKEN_TYPE_DELIM ||
              token.type == N1_CSV_TOKEN_TYPE_ROW){
          
        if(!*is_quoted){
          
          n1_CSV_Cell cell = {
            *cell_start,
            token.offset
          };
          
          parser->cell_data[parser->cell_count++] = cell;
          n1_csv_internal_maybe_realloc_cell_data(parser);
            
          *start_of_cell = true;
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
  return true;
}
static void n1_csv_parse_threaded_internal(n1_CSV_Parser* parser,
                                           char delim_token,
                                           char quote_token,
                                           char row_token,
                                           void (*threadproc)(n1_CSV_Parser*, n1_CSV_TokenStream*, char, char, char, char*, size_t, size_t)){
  if(!parser->file_size){
    return;
  }
  
#define THREAD_COUNT 16
  
#if defined(__linux__)
  pthread_t threads[THREAD_COUNT];
#elif defined(_WIN32)
  HANDLE threads[THREAD_COUNT];  
#endif
  
  n1_CSV_ParseInfo infos[THREAD_COUNT];

  size_t bytes_to_read = (parser->file_size / THREAD_COUNT);
  bytes_to_read += 32 - (bytes_to_read % 32);
  
  size_t offset = 0;

  for(int i = 0; i < THREAD_COUNT; i++){
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
    pthread_create(&threads[i], nullptr, (void*(*)(void*))n1_csv_tokenize_paged, &infos[i]);
#elif defined(_WIN32)
    DWORD id;
    threads[i] = CreateThread(NULL, 0, (DWORD(*)(void*))n1_csv_tokenize_paged, &infos[i], 0, &id);
#endif
  }
  
  parser->column_count  = 256;
  parser->row_count     = 1;
  parser->cell_data     = (n1_CSV_Cell*)n1_csv_malloc(sizeof(n1_CSV_Cell) * parser->column_count);

  //--------------------------

  n1_CSV_Token prev_token        = {};
  bool         is_quoted         = false;
  uint32_t     cell_start        = 0;
  uint32_t     row_idx           = 0;
  bool         keep_processing   = true;
  int          start_quote_count = 0;
  int          end_quote_count   = 0;
  bool         start_of_cell     = true;

  bool run = true;
  for(int i = 0; i < THREAD_COUNT; i++){
    
#if defined(__linux__)
    pthread_join(threads[i], nullptr);
#elif defined(_WIN32)
    WaitForSingleObject(threads[i], INFINITE);
#endif
    if(run)
      run = n1_csv_parse_tokens_internal(parser,
                                         infos[i].tokens.token_count,
                                         infos[i].tokens.tokens,
                                         &prev_token,
                                         &is_quoted,
                                         &cell_start,
                                         &row_idx,
                                         &start_quote_count,
                                         &end_quote_count,
                                         &start_of_cell);
    n1_csv_free(infos[i].tokens.tokens);
  }
}

N1_CSV_STATIC_API void n1_csv_parse_threaded_slow(n1_CSV_Parser* parser,
                                                  char delim_token,
                                                  char quote_token,
                                                  char row_token){
  n1_csv_parse_threaded_internal(parser,
                                 delim_token,
                                 quote_token,
                                 row_token,
                                 n1_csv_tokenize_slow);
}

N1_CSV_STATIC_API void n1_csv_parse_threaded_sse2(n1_CSV_Parser* parser,
                                                  char delim_token,
                                                  char quote_token,
                                                  char row_token){
  n1_csv_parse_threaded_internal(parser,
                                 delim_token,
                                 quote_token,
                                 row_token,
                                 n1_csv_tokenize_sse2);
}

N1_CSV_STATIC_API void n1_csv_parse_threaded_avx256(n1_CSV_Parser* parser,
                                                    char delim_token,
                                                    char quote_token,
                                                    char row_token){
  n1_csv_parse_threaded_internal(parser,
                                 delim_token,
                                 quote_token,
                                 row_token,
                                 n1_csv_tokenize_avx256);
}
#endif
#endif
