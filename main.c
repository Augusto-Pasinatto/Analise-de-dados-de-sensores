/*
 * ============================================================
 * Analise de dados dos sensores - CityLivingLab
 * Disciplina: Fundamentos de Sistemas Operacionais
 * ============================================================
 *
 * Programa em C usando pthreads e cJSON para processar arquivos
 * JSON com medicoes de sensores de Caxias do Sul e Bento Goncalves.
 *
 * Como instalar dependencias no Xubuntu/Ubuntu:
 *
 * Opcao 1 - usando pacote do sistema:
 *   sudo apt update
 *   sudo apt install gcc libcjson-dev
 *   gcc main.c -o sensores -lpthread -lcjson
 *
 * Opcao 2 - usando cJSON.c e cJSON.h na mesma pasta:
 *   Baixe cJSON.c e cJSON.h de https://github.com/DaveGamble/cJSON
 *   gcc main.c cJSON.c -o sensores -lpthread
 *
 * Como executar:
 *   ./sensores
 *
 * Por padrao o programa tenta ler:
 *   sensores_caxias.json
 *   sensores_bento.json
 *
 * Tambem e possivel informar os nomes pela linha de comando:
 *   ./sensores arquivo1.json arquivo2.json
 *
 * Threads utilizadas:
 *   Thread 1: leitura dos dados e eliminacao de duplicatas
 *   Thread 2: calculo das estatisticas
 *   Thread 3: registro das mensagens de log em processamento.log
 */
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <locale.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>
#if defined(__has_include)
#  if __has_include(<cjson/cJSON.h>)
#    include <cjson/cJSON.h>
#  else
#    include "cJSON.h"
#  endif
#else
#  include "cJSON.h"
#endif
#define DEFAULT_FILE_CAXIAS "sensores_caxias.json"
#define DEFAULT_FILE_BENTO  "sensores_bento.json"
#define LOG_FILE_NAME       "processamento.log"
#define NUM_INPUT_FILES 2
#define NUM_CITIES 2
#define CITY_CAXIAS 0
#define CITY_BENTO  1
#define CITY_UNKNOWN -1
#define MAX_FILENAME_LEN 256
#define MAX_SMALL_TEXT   128
#define MAX_TEXT         256
#define MAX_TIME_TEXT     64
#define MAX_LOG_TEXT     512
#define MAX_SF_VALUES     64
typedef struct {
    int city;
    int file_index;
    char source_file[MAX_FILENAME_LEN];
    char duplicate_key[MAX_TEXT];
    char device_id[MAX_TEXT];
    char device_name[MAX_TEXT];
    char timestamp_iso[MAX_TIME_TEXT];
    char timestamp_key[MAX_TIME_TEXT];
    char timestamp_br[MAX_TIME_TEXT];
    int has_temperature;
    int has_humidity;
    int has_pressure;
    int has_battery;
    int has_sf;
    double temperature;
    double humidity;
    double pressure;
    double battery;
    int spreading_factor;
} Measurement;
typedef struct {
    int count;
    double min_value;
    double max_value;
    double sum;
    char min_time[MAX_TIME_TEXT];
    char max_time[MAX_TIME_TEXT];
} MetricStats;
typedef struct {
    char city_name[MAX_SMALL_TEXT];
    int total_records;
    char period_start_key[MAX_TIME_TEXT];
    char period_end_key[MAX_TIME_TEXT];
    char period_start_br[MAX_TIME_TEXT];
    char period_end_br[MAX_TIME_TEXT];
    MetricStats temperature;
    MetricStats humidity;
    MetricStats pressure;
    int battery_count;
    double battery_initial;
    double battery_final;
    char battery_initial_key[MAX_TIME_TEXT];
    char battery_final_key[MAX_TIME_TEXT];
    int sf_values[MAX_SF_VALUES];
    int sf_count;
} CityStats;
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int raw_records;
    int processed_records;
    int duplicate_records;
    int invalid_records;
    char period_start_key[MAX_TIME_TEXT];
    char period_end_key[MAX_TIME_TEXT];
    char period_start_br[MAX_TIME_TEXT];
    char period_end_br[MAX_TIME_TEXT];
} FileSummary;
typedef struct LogNode {
    char *message;
    struct LogNode *next;
} LogNode;
typedef struct {
    LogNode *head;
    LogNode *tail;
    int closed;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} LogQueue;
typedef struct {
    char **keys;
    size_t capacity;
    size_t count;
} StringSet;
typedef struct {
    char input_files[NUM_INPUT_FILES][MAX_FILENAME_LEN];
    FileSummary file_summaries[NUM_INPUT_FILES];
    Measurement *measurements;
    size_t measurement_count;
    size_t measurement_capacity;
    CityStats city_stats[NUM_CITIES];
    pthread_mutex_t data_mutex;
    pthread_cond_t data_ready;
    int reading_finished;
    LogQueue log_queue;
} ProgramContext;
static void *thread_leitura(void *arg);
static void *thread_estatisticas(void *arg);
static void *thread_logs(void *arg);
static void safe_copy(char *dest, size_t size, const char *src) {
    if (dest == NULL || size == 0) {
        return;
    }
    if (src == NULL) {
        dest[0] = '\0';
        return;
    }
    snprintf(dest, size, "%s", src);
}
static char *xstrdup(const char *text) {
    size_t len;
    char *copy;
    if (text == NULL) {
        return NULL;
    }
    len = strlen(text);
    copy = (char *)malloc(len + 1);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, text, len + 1);
    return copy;
}
static int contains_case_insensitive(const char *text, const char *needle) {
    size_t text_len;
    size_t needle_len;
    size_t i;
    if (text == NULL || needle == NULL) {
        return 0;
    }
    text_len = strlen(text);
    needle_len = strlen(needle);
    if (needle_len == 0 || needle_len > text_len) {
        return 0;
    }
    for (i = 0; i <= text_len - needle_len; i++) {
        if (strncasecmp(text + i, needle, needle_len) == 0) {
            return 1;
        }
    }
    return 0;
}
static int detectar_cidade(const char *device_name) {
    if (contains_case_insensitive(device_name, "Caxias")) {
        return CITY_CAXIAS;
    }
    if (contains_case_insensitive(device_name, "Bento")) {
        return CITY_BENTO;
    }
    return CITY_UNKNOWN;
}
static void timestamp_to_key(const char *iso, char *out, size_t out_size) {
    size_t i;
    size_t len;
    if (out == NULL || out_size == 0) {
        return;
    }
    out[0] = '\0';
    if (iso == NULL) {
        return;
    }
    len = strlen(iso);
    if (len < 10) {
        return;
    }
    for (i = 0; i < out_size - 1 && i < len && i < 19; i++) {
        out[i] = (iso[i] == ' ') ? 'T' : iso[i];
    }
    out[i] = '\0';
}
static void converter_timestamp_iso_para_br(const char *iso, char *out, size_t out_size) {
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
    if (out == NULL || out_size == 0) {
        return;
    }
    out[0] = '\0';
    if (iso == NULL || strlen(iso) < 10) {
        safe_copy(out, out_size, "-");
        return;
    }
    hour = 0;
    minute = 0;
    second = 0;
    if (sscanf(iso, "%4d-%2d-%2dT%2d:%2d:%2d",
               &year, &month, &day, &hour, &minute, &second) >= 3 ||
        sscanf(iso, "%4d-%2d-%2d %2d:%2d:%2d",
               &year, &month, &day, &hour, &minute, &second) >= 3) {
        snprintf(out, out_size, "%02d/%02d/%04d %02d:%02d:%02d",
                 day, month, year, hour, minute, second);
    } else {
        safe_copy(out, out_size, iso);
    }
}
static void converter_data_iso_para_br(const char *iso, char *out, size_t out_size) {
    int year;
    int month;
    int day;
    if (out == NULL || out_size == 0) {
        return;
    }
    out[0] = '\0';
    if (iso == NULL || strlen(iso) < 10) {
        safe_copy(out, out_size, "-");
        return;
    }
    if (sscanf(iso, "%4d-%2d-%2d", &year, &month, &day) == 3) {
        snprintf(out, out_size, "%02d/%02d/%04d", day, month, year);
    } else {
        safe_copy(out, out_size, iso);
    }
}
static int timestamp_is_before(const char *a, const char *b) {
    if (a == NULL || a[0] == '\0') {
        return 0;
    }
    if (b == NULL || b[0] == '\0') {
        return 1;
    }
    return strcmp(a, b) < 0;
}
static int timestamp_is_after(const char *a, const char *b) {
    if (a == NULL || a[0] == '\0') {
        return 0;
    }
    if (b == NULL || b[0] == '\0') {
        return 1;
    }
    return strcmp(a, b) > 0;
}
static int parse_double_from_json_value(const cJSON *value_item, double *out) {
    char *endptr;
    double value;
    if (out == NULL || value_item == NULL) {
        return 0;
    }
    if (cJSON_IsNumber(value_item)) {
        *out = value_item->valuedouble;
        return 1;
    }
    if (cJSON_IsString(value_item) && value_item->valuestring != NULL) {
        errno = 0;
        value = strtod(value_item->valuestring, &endptr);
        if (errno == 0 && endptr != value_item->valuestring) {
            *out = value;
            return 1;
        }
    }
    return 0;
}
static int get_json_scalar_text(const cJSON *object, const char *field, char *out, size_t out_size) {
    const cJSON *item;
    if (object == NULL || field == NULL || out == NULL || out_size == 0) {
        return 0;
    }
    out[0] = '\0';
    item = cJSON_GetObjectItemCaseSensitive((cJSON *)object, field);
    if (item == NULL) {
        return 0;
    }
    if (cJSON_IsString(item) && item->valuestring != NULL) {
        safe_copy(out, out_size, item->valuestring);
        return out[0] != '\0';
    }
    if (cJSON_IsNumber(item)) {
        snprintf(out, out_size, "%.0f", item->valuedouble);
        return 1;
    }
    if (cJSON_IsBool(item)) {
        safe_copy(out, out_size, cJSON_IsTrue(item) ? "true" : "false");
        return 1;
    }
    return 0;
}
static int get_first_existing_text(const cJSON *object,
                                   const char **fields,
                                   int field_count,
                                   char *out,
                                   size_t out_size) {
    int i;
    if (out == NULL || out_size == 0) {
        return 0;
    }
    out[0] = '\0';
    for (i = 0; i < field_count; i++) {
        if (get_json_scalar_text(object, fields[i], out, out_size)) {
            return 1;
        }
    }
    return 0;
}
static void log_queue_init(LogQueue *queue) {
    queue->head = NULL;
    queue->tail = NULL;
    queue->closed = 0;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}
static void log_queue_destroy(LogQueue *queue) {
    LogNode *node;
    LogNode *next;
    if (queue == NULL) {
        return;
    }
    node = queue->head;
    while (node != NULL) {
        next = node->next;
        free(node->message);
        free(node);
        node = next;
    }
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
}
static void log_queue_close(LogQueue *queue) {
    if (queue == NULL) {
        return;
    }
    pthread_mutex_lock(&queue->mutex);
    queue->closed = 1;
    pthread_cond_broadcast(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}
static void adicionar_log(ProgramContext *ctx, const char *fmt, ...) {
    char buffer[MAX_LOG_TEXT];
    char final_message[MAX_LOG_TEXT + 64];
    time_t now;
    struct tm local_tm;
    char time_text[32];
    va_list args;
    LogNode *node;
    if (ctx == NULL || fmt == NULL) {
        return;
    }
    va_start(args, fmt);
    vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    now = time(NULL);
    localtime_r(&now, &local_tm);
    strftime(time_text, sizeof(time_text), "%Y-%m-%d %H:%M:%S", &local_tm);
    snprintf(final_message, sizeof(final_message), "[%s] %s", time_text, buffer);
    node = (LogNode *)malloc(sizeof(LogNode));
    if (node == NULL) {
        return;
    }
    node->message = xstrdup(final_message);
    node->next = NULL;
    if (node->message == NULL) {
        free(node);
        return;
    }
    pthread_mutex_lock(&ctx->log_queue.mutex);
    if (ctx->log_queue.closed) {
        pthread_mutex_unlock(&ctx->log_queue.mutex);
        free(node->message);
        free(node);
        return;
    }
    if (ctx->log_queue.tail == NULL) {
        ctx->log_queue.head = node;
        ctx->log_queue.tail = node;
    } else {
        ctx->log_queue.tail->next = node;
        ctx->log_queue.tail = node;
    }
    pthread_cond_signal(&ctx->log_queue.cond);
    pthread_mutex_unlock(&ctx->log_queue.mutex);
}
static char *log_queue_pop(LogQueue *queue) {
    LogNode *node;
    char *message;
    pthread_mutex_lock(&queue->mutex);
    while (queue->head == NULL && !queue->closed) {
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }
    if (queue->head == NULL && queue->closed) {
        pthread_mutex_unlock(&queue->mutex);
        return NULL;
    }
    node = queue->head;
    queue->head = node->next;
    if (queue->head == NULL) {
        queue->tail = NULL;
    }
    pthread_mutex_unlock(&queue->mutex);
    message = node->message;
    free(node);
    return message;
}
static unsigned long hash_string(const char *text) {
    unsigned long hash = 1469598103934665603UL;
    const unsigned char *ptr = (const unsigned char *)text;
    while (ptr != NULL && *ptr != '\0') {
        hash ^= (unsigned long)(*ptr);
        hash *= 1099511628211UL;
        ptr++;
    }
    return hash;
}
static int string_set_init(StringSet *set, size_t initial_capacity) {
    size_t i;
    if (set == NULL) {
        return 0;
    }
    if (initial_capacity < 128) {
        initial_capacity = 128;
    }
    set->keys = (char **)malloc(sizeof(char *) * initial_capacity);
    if (set->keys == NULL) {
        return 0;
    }
    set->capacity = initial_capacity;
    set->count = 0;
    for (i = 0; i < set->capacity; i++) {
        set->keys[i] = NULL;
    }
    return 1;
}
static void string_set_free(StringSet *set) {
    size_t i;
    if (set == NULL || set->keys == NULL) {
        return;
    }
    for (i = 0; i < set->capacity; i++) {
        free(set->keys[i]);
    }
    free(set->keys);
    set->keys = NULL;
    set->capacity = 0;
    set->count = 0;
}
static int string_set_insert_existing_copy(StringSet *set, char *key_copy) {
    unsigned long hash;
    size_t index;
    size_t probes;
    if (set == NULL || key_copy == NULL || set->keys == NULL) {
        return 0;
    }
    hash = hash_string(key_copy);
    index = hash % set->capacity;
    for (probes = 0; probes < set->capacity; probes++) {
        if (set->keys[index] == NULL) {
            set->keys[index] = key_copy;
            set->count++;
            return 1;
        }
        index = (index + 1) % set->capacity;
    }
    return 0;
}
static int string_set_grow(StringSet *set) {
    StringSet grown;
    size_t old_capacity;
    char **old_keys;
    size_t i;
    if (set == NULL || set->keys == NULL) {
        return 0;
    }
    old_capacity = set->capacity;
    old_keys = set->keys;
    if (!string_set_init(&grown, old_capacity * 2)) {
        return 0;
    }
    for (i = 0; i < old_capacity; i++) {
        if (old_keys[i] != NULL) {
            if (!string_set_insert_existing_copy(&grown, old_keys[i])) {
                free(grown.keys);
                set->keys = old_keys;
                return 0;
            }
            old_keys[i] = NULL;
        }
    }
    free(old_keys);
    set->keys = grown.keys;
    set->capacity = grown.capacity;
    set->count = grown.count;
    return 1;
}
static int string_set_add(StringSet *set, const char *key) {
    unsigned long hash;
    size_t index;
    size_t probes;
    char *copy;
    if (set == NULL || key == NULL || key[0] == '\0') {
        return 0;
    }
    if ((set->count + 1) * 100 / set->capacity > 70) {
        if (!string_set_grow(set)) {
            return 0;
        }
    }
    hash = hash_string(key);
    index = hash % set->capacity;
    for (probes = 0; probes < set->capacity; probes++) {
        if (set->keys[index] == NULL) {
            copy = xstrdup(key);
            if (copy == NULL) {
                return 0;
            }
            set->keys[index] = copy;
            set->count++;
            return 1;
        }
        if (strcmp(set->keys[index], key) == 0) {
            return 0;
        }
        index = (index + 1) % set->capacity;
    }
    return 0;
}
static char *read_entire_file(const char *filename, long *out_size) {
    FILE *file;
    long size;
    char *buffer;
    size_t read_count;
    if (out_size != NULL) {
        *out_size = 0;
    }
    file = fopen(filename, "rb");
    if (file == NULL) {
        return NULL;
    }
    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }
    size = ftell(file);
    if (size < 0) {
        fclose(file);
        return NULL;
    }
    rewind(file);
    buffer = (char *)malloc((size_t)size + 1);
    if (buffer == NULL) {
        fclose(file);
        return NULL;
    }
    read_count = fread(buffer, 1, (size_t)size, file);
    fclose(file);
    if (read_count != (size_t)size) {
        free(buffer);
        return NULL;
    }
    buffer[size] = '\0';
    if (out_size != NULL) {
        *out_size = size;
    }
    return buffer;
}
static int ensure_measurement_capacity(ProgramContext *ctx) {
    Measurement *grown;
    size_t new_capacity;
    if (ctx == NULL) {
        return 0;
    }
    if (ctx->measurement_count < ctx->measurement_capacity) {
        return 1;
    }
    new_capacity = (ctx->measurement_capacity == 0) ? 1024 : ctx->measurement_capacity * 2;
    grown = (Measurement *)realloc(ctx->measurements, sizeof(Measurement) * new_capacity);
    if (grown == NULL) {
        return 0;
    }
    ctx->measurements = grown;
    ctx->measurement_capacity = new_capacity;
    return 1;
}
static int add_measurement(ProgramContext *ctx, const Measurement *measurement) {
    int ok;
    if (ctx == NULL || measurement == NULL) {
        return 0;
    }
    pthread_mutex_lock(&ctx->data_mutex);
    ok = ensure_measurement_capacity(ctx);
    if (ok) {
        ctx->measurements[ctx->measurement_count] = *measurement;
        ctx->measurement_count++;
    }
    pthread_mutex_unlock(&ctx->data_mutex);
    return ok;
}
static int variable_is_temperature(const char *variable) {
    return variable != NULL &&
           (strcasecmp(variable, "temperature") == 0 ||
            strcasecmp(variable, "temp") == 0);
}
static int variable_is_humidity(const char *variable) {
    return variable != NULL &&
           (strcasecmp(variable, "humidity") == 0 ||
            strcasecmp(variable, "umidade") == 0);
}
static int variable_is_pressure(const char *variable) {
    return variable != NULL &&
           (strcasecmp(variable, "airpressure") == 0 ||
            strcasecmp(variable, "pressure") == 0 ||
            strcasecmp(variable, "atmospheric_pressure") == 0 ||
            strcasecmp(variable, "barometric_pressure") == 0);
}
static int variable_is_battery(const char *variable) {
    return variable != NULL &&
           (strcasecmp(variable, "batterylevel") == 0 ||
            strcasecmp(variable, "battery") == 0 ||
            strcasecmp(variable, "battery_level") == 0);
}
static int variable_is_spreading_factor(const char *variable) {
    if (variable == NULL) {
        return 0;
    }
    return strcasecmp(variable, "sf") == 0 ||
           strcasecmp(variable, "lora_sf") == 0 ||
           strcasecmp(variable, "spreading_factor") == 0 ||
           strcasecmp(variable, "lora_spreading_factor") == 0 ||
           contains_case_insensitive(variable, "spreading");
}
static void remember_first_timestamp(char *dest, size_t dest_size, const char *candidate) {
    if (dest == NULL || dest_size == 0 || dest[0] != '\0') {
        return;
    }
    if (candidate != NULL && candidate[0] != '\0') {
        safe_copy(dest, dest_size, candidate);
    }
}
static void extract_measurement_data(const cJSON *internal_json,
                                     Measurement *measurement,
                                     char *fallback_time,
                                     size_t fallback_time_size) {
    const cJSON *data_array;
    const cJSON *item;
    const cJSON *variable_item;
    const cJSON *value_item;
    const cJSON *time_item;
    const char *variable;
    const char *item_time;
    double number;
    if (internal_json == NULL || measurement == NULL) {
        return;
    }
    data_array = cJSON_GetObjectItemCaseSensitive((cJSON *)internal_json, "data");
    if (!cJSON_IsArray(data_array)) {
        return;
    }
    cJSON_ArrayForEach(item, data_array) {
        if (!cJSON_IsObject(item)) {
            continue;
        }
        variable_item = cJSON_GetObjectItemCaseSensitive((cJSON *)item, "variable");
        value_item = cJSON_GetObjectItemCaseSensitive((cJSON *)item, "value");
        time_item = cJSON_GetObjectItemCaseSensitive((cJSON *)item, "time");
        if (!cJSON_IsString(variable_item) || variable_item->valuestring == NULL) {
            continue;
        }
        variable = variable_item->valuestring;
        item_time = (cJSON_IsString(time_item) && time_item->valuestring != NULL)
                        ? time_item->valuestring
                        : NULL;
        remember_first_timestamp(fallback_time, fallback_time_size, item_time);
        if (variable_is_temperature(variable)) {
            if (parse_double_from_json_value(value_item, &number)) {
                measurement->temperature = number;
                measurement->has_temperature = 1;
                remember_first_timestamp(measurement->timestamp_iso,
                                         sizeof(measurement->timestamp_iso),
                                         item_time);
            }
        } else if (variable_is_humidity(variable)) {
            if (parse_double_from_json_value(value_item, &number)) {
                measurement->humidity = number;
                measurement->has_humidity = 1;
                remember_first_timestamp(measurement->timestamp_iso,
                                         sizeof(measurement->timestamp_iso),
                                         item_time);
            }
        } else if (variable_is_pressure(variable)) {
            if (parse_double_from_json_value(value_item, &number)) {
                measurement->pressure = number;
                measurement->has_pressure = 1;
                remember_first_timestamp(measurement->timestamp_iso,
                                         sizeof(measurement->timestamp_iso),
                                         item_time);
            }
        } else if (variable_is_battery(variable)) {
            if (parse_double_from_json_value(value_item, &number)) {
                measurement->battery = number;
                measurement->has_battery = 1;
                remember_first_timestamp(measurement->timestamp_iso,
                                         sizeof(measurement->timestamp_iso),
                                         item_time);
            }
        } else if (variable_is_spreading_factor(variable)) {
            if (parse_double_from_json_value(value_item, &number)) {
                measurement->spreading_factor = (int)(number + (number >= 0 ? 0.5 : -0.5));
                measurement->has_sf = 1;
                remember_first_timestamp(measurement->timestamp_iso,
                                         sizeof(measurement->timestamp_iso),
                                         item_time);
            }
        }
    }
}
static int get_inner_json_string(const cJSON *external, const char **out_text, const char **out_field) {
    const cJSON *item;
    if (out_text != NULL) {
        *out_text = NULL;
    }
    if (out_field != NULL) {
        *out_field = NULL;
    }
    if (external == NULL) {
        return 0;
    }
    item = cJSON_GetObjectItemCaseSensitive((cJSON *)external, "brute_data");
    if (cJSON_IsString(item) && item->valuestring != NULL) {
        if (out_text != NULL) {
            *out_text = item->valuestring;
        }
        if (out_field != NULL) {
            *out_field = "brute_data";
        }
        return 1;
    }
    item = cJSON_GetObjectItemCaseSensitive((cJSON *)external, "payload");
    if (cJSON_IsString(item) && item->valuestring != NULL) {
        if (out_text != NULL) {
            *out_text = item->valuestring;
        }
        if (out_field != NULL) {
            *out_field = "payload";
        }
        return 1;
    }
    return 0;
}
static void build_duplicate_key(const cJSON *external,
                                const Measurement *measurement,
                                char *out,
                                size_t out_size) {
    char id_text[MAX_SMALL_TEXT];
    if (out == NULL || out_size == 0) {
        return;
    }
    out[0] = '\0';
    if (get_json_scalar_text(external, "payload_id", id_text, sizeof(id_text))) {
        snprintf(out, out_size, "payload_id:%s", id_text);
        return;
    }
    if (get_json_scalar_text(external, "id", id_text, sizeof(id_text))) {
        snprintf(out, out_size, "id:%s", id_text);
        return;
    }
    if (measurement != NULL &&
        measurement->device_id[0] != '\0' &&
        measurement->timestamp_key[0] != '\0') {
        snprintf(out, out_size, "device_time:%s:%s",
                 measurement->device_id,
                 measurement->timestamp_key);
    }
}
static int parse_external_record(ProgramContext *ctx,
                                 const cJSON *external,
                                 int file_index,
                                 Measurement *measurement) {
    const char *timestamp_fields[] = {"payload_date", "received_at", "created_at", "time", "timestamp"};
    const char *inner_text;
    const char *inner_field;
    char external_time[MAX_TIME_TEXT];
    char fallback_time[MAX_TIME_TEXT];
    cJSON *internal_json;
    const cJSON *device_id_item;
    const cJSON *device_name_item;
    if (ctx == NULL || external == NULL || measurement == NULL) {
        return 0;
    }
    memset(measurement, 0, sizeof(Measurement));
    measurement->file_index = file_index;
    safe_copy(measurement->source_file,
              sizeof(measurement->source_file),
              ctx->input_files[file_index]);
    if (!cJSON_IsObject(external)) {
        adicionar_log(ctx, "Registro ignorado em %s: item externo nao e objeto JSON.",
                      ctx->input_files[file_index]);
        return 0;
    }
    if (!get_inner_json_string(external, &inner_text, &inner_field)) {
        adicionar_log(ctx, "Registro ignorado em %s: campos brute_data/payload ausentes ou invalidos.",
                      ctx->input_files[file_index]);
        return 0;
    }
    internal_json = cJSON_Parse(inner_text);
    if (internal_json == NULL) {
        adicionar_log(ctx, "Registro ignorado em %s: JSON interno em %s e invalido.",
                      ctx->input_files[file_index], inner_field);
        return 0;
    }
    if (!cJSON_IsObject(internal_json)) {
        adicionar_log(ctx, "Registro ignorado em %s: JSON interno em %s nao e objeto.",
                      ctx->input_files[file_index], inner_field);
        cJSON_Delete(internal_json);
        return 0;
    }
    device_id_item = cJSON_GetObjectItemCaseSensitive(internal_json, "device_id");
    if (cJSON_IsString(device_id_item) && device_id_item->valuestring != NULL) {
        safe_copy(measurement->device_id,
                  sizeof(measurement->device_id),
                  device_id_item->valuestring);
    }
    device_name_item = cJSON_GetObjectItemCaseSensitive(internal_json, "device_name");
    if (cJSON_IsString(device_name_item) && device_name_item->valuestring != NULL) {
        safe_copy(measurement->device_name,
                  sizeof(measurement->device_name),
                  device_name_item->valuestring);
    }
    measurement->city = detectar_cidade(measurement->device_name);
    if (measurement->city == CITY_UNKNOWN) {
        adicionar_log(ctx, "Registro ignorado em %s: cidade nao identificada em device_name='%s'.",
                      ctx->input_files[file_index],
                      measurement->device_name[0] ? measurement->device_name : "(ausente)");
        cJSON_Delete(internal_json);
        return 0;
    }
    fallback_time[0] = '\0';
    extract_measurement_data(internal_json, measurement, fallback_time, sizeof(fallback_time));
    if (measurement->timestamp_iso[0] == '\0') {
        safe_copy(measurement->timestamp_iso, sizeof(measurement->timestamp_iso), fallback_time);
    }
    external_time[0] = '\0';
    get_first_existing_text(external, timestamp_fields,
                            (int)(sizeof(timestamp_fields) / sizeof(timestamp_fields[0])),
                            external_time, sizeof(external_time));
    if (measurement->timestamp_iso[0] == '\0') {
        safe_copy(measurement->timestamp_iso, sizeof(measurement->timestamp_iso), external_time);
    }
    timestamp_to_key(measurement->timestamp_iso,
                     measurement->timestamp_key,
                     sizeof(measurement->timestamp_key));
    converter_timestamp_iso_para_br(measurement->timestamp_iso,
                                    measurement->timestamp_br,
                                    sizeof(measurement->timestamp_br));
    if (!measurement->has_temperature &&
        !measurement->has_humidity &&
        !measurement->has_pressure &&
        !measurement->has_battery &&
        !measurement->has_sf) {
        adicionar_log(ctx, "Registro ignorado em %s: nenhum campo util encontrado no array data.",
                      ctx->input_files[file_index]);
        cJSON_Delete(internal_json);
        return 0;
    }
    if (!measurement->has_temperature ||
        !measurement->has_humidity ||
        !measurement->has_pressure ||
        !measurement->has_battery) {
        adicionar_log(ctx,
                      "Registro incompleto em %s: device='%s', tempo='%s', temp=%s, umidade=%s, pressao=%s, bateria=%s.",
                      ctx->input_files[file_index],
                      measurement->device_name,
                      measurement->timestamp_iso[0] ? measurement->timestamp_iso : "(sem tempo)",
                      measurement->has_temperature ? "ok" : "ausente",
                      measurement->has_humidity ? "ok" : "ausente",
                      measurement->has_pressure ? "ok" : "ausente",
                      measurement->has_battery ? "ok" : "ausente");
    }
    build_duplicate_key(external, measurement,
                        measurement->duplicate_key,
                        sizeof(measurement->duplicate_key));
    if (measurement->duplicate_key[0] == '\0') {
        adicionar_log(ctx, "Registro ignorado em %s: nao foi possivel montar chave de duplicidade.",
                      ctx->input_files[file_index]);
        cJSON_Delete(internal_json);
        return 0;
    }
    cJSON_Delete(internal_json);
    return 1;
}
static void process_single_external_record(ProgramContext *ctx,
                                           const cJSON *external,
                                           int file_index,
                                           StringSet *seen_keys) {
    Measurement measurement;
    FileSummary *summary;
    if (ctx == NULL || external == NULL || seen_keys == NULL) {
        return;
    }
    summary = &ctx->file_summaries[file_index];
    summary->raw_records++;
    if (!parse_external_record(ctx, external, file_index, &measurement)) {
        summary->invalid_records++;
        return;
    }
    if (!string_set_add(seen_keys, measurement.duplicate_key)) {
        summary->duplicate_records++;
        adicionar_log(ctx, "Duplicata ignorada em %s: chave='%s'.",
                      ctx->input_files[file_index], measurement.duplicate_key);
        return;
    }
    if (!add_measurement(ctx, &measurement)) {
        summary->invalid_records++;
        adicionar_log(ctx, "Falha de memoria ao armazenar registro de %s.",
                      ctx->input_files[file_index]);
        return;
    }
    summary->processed_records++;
}
static void process_json_file(ProgramContext *ctx, int file_index, StringSet *seen_keys) {
    char *content;
    long file_size;
    cJSON *root;
    cJSON *item;
    const char *filename;
    if (ctx == NULL || seen_keys == NULL) {
        return;
    }
    filename = ctx->input_files[file_index];
    adicionar_log(ctx, "Iniciando leitura do arquivo %s.", filename);
    content = read_entire_file(filename, &file_size);
    if (content == NULL) {
        adicionar_log(ctx, "Erro ao abrir/ler arquivo %s.", filename);
        ctx->file_summaries[file_index].invalid_records++;
        return;
    }
    adicionar_log(ctx, "Arquivo %s lido com %ld bytes.", filename, file_size);
    root = cJSON_Parse(content);
    free(content);
    if (root == NULL) {
        adicionar_log(ctx, "JSON invalido no arquivo %s.", filename);
        ctx->file_summaries[file_index].invalid_records++;
        return;
    }
    if (cJSON_IsArray(root)) {
        cJSON_ArrayForEach(item, root) {
            process_single_external_record(ctx, item, file_index, seen_keys);
        }
    } else if (cJSON_IsObject(root)) {
        process_single_external_record(ctx, root, file_index, seen_keys);
    } else {
        adicionar_log(ctx, "Arquivo %s ignorado: raiz JSON nao e array nem objeto.", filename);
        ctx->file_summaries[file_index].invalid_records++;
    }
    cJSON_Delete(root);
    adicionar_log(ctx,
                  "Leitura concluida de %s: brutos=%d, validos=%d, duplicados=%d, invalidos=%d.",
                  filename,
                  ctx->file_summaries[file_index].raw_records,
                  ctx->file_summaries[file_index].processed_records,
                  ctx->file_summaries[file_index].duplicate_records,
                  ctx->file_summaries[file_index].invalid_records);
}
static void metric_stats_init(MetricStats *stats) {
    if (stats == NULL) {
        return;
    }
    stats->count = 0;
    stats->min_value = DBL_MAX;
    stats->max_value = -DBL_MAX;
    stats->sum = 0.0;
    safe_copy(stats->min_time, sizeof(stats->min_time), "-");
    safe_copy(stats->max_time, sizeof(stats->max_time), "-");
}
static void metric_stats_add(MetricStats *stats, double value, const char *time_br) {
    if (stats == NULL) {
        return;
    }
    if (stats->count == 0 || value < stats->min_value) {
        stats->min_value = value;
        safe_copy(stats->min_time, sizeof(stats->min_time), time_br);
    }
    if (stats->count == 0 || value > stats->max_value) {
        stats->max_value = value;
        safe_copy(stats->max_time, sizeof(stats->max_time), time_br);
    }
    stats->sum += value;
    stats->count++;
}
static void city_stats_init(CityStats *stats, const char *city_name) {
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(CityStats));
    safe_copy(stats->city_name, sizeof(stats->city_name), city_name);
    safe_copy(stats->period_start_br, sizeof(stats->period_start_br), "-");
    safe_copy(stats->period_end_br, sizeof(stats->period_end_br), "-");
    metric_stats_init(&stats->temperature);
    metric_stats_init(&stats->humidity);
    metric_stats_init(&stats->pressure);
}
static void update_period(char *start_key,
                          char *end_key,
                          char *start_br,
                          char *end_br,
                          const Measurement *measurement) {
    if (measurement == NULL || measurement->timestamp_key[0] == '\0') {
        return;
    }
    if (timestamp_is_before(measurement->timestamp_key, start_key)) {
        safe_copy(start_key, MAX_TIME_TEXT, measurement->timestamp_key);
        converter_data_iso_para_br(measurement->timestamp_key, start_br, MAX_TIME_TEXT);
    }
    if (timestamp_is_after(measurement->timestamp_key, end_key)) {
        safe_copy(end_key, MAX_TIME_TEXT, measurement->timestamp_key);
        converter_data_iso_para_br(measurement->timestamp_key, end_br, MAX_TIME_TEXT);
    }
}
static int city_has_sf(const CityStats *stats, int sf) {
    int i;
    if (stats == NULL) {
        return 0;
    }
    for (i = 0; i < stats->sf_count; i++) {
        if (stats->sf_values[i] == sf) {
            return 1;
        }
    }
    return 0;
}
static void city_add_sf(CityStats *stats, int sf) {
    if (stats == NULL) {
        return;
    }
    if (city_has_sf(stats, sf)) {
        return;
    }
    if (stats->sf_count < MAX_SF_VALUES) {
        stats->sf_values[stats->sf_count] = sf;
        stats->sf_count++;
    }
}
static int compare_ints(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    return (ia > ib) - (ia < ib);
}
static void sort_sf_values(CityStats *stats) {
    if (stats == NULL || stats->sf_count <= 1) {
        return;
    }
    qsort(stats->sf_values, (size_t)stats->sf_count, sizeof(int), compare_ints);
}
static void calcular_estatisticas(ProgramContext *ctx) {
    size_t i;
    Measurement *m;
    CityStats *city_stats;
    FileSummary *file_summary;
    if (ctx == NULL) {
        return;
    }
    city_stats_init(&ctx->city_stats[CITY_CAXIAS], "Caxias do Sul");
    city_stats_init(&ctx->city_stats[CITY_BENTO], "Bento Gonçalves");
    for (i = 0; i < ctx->measurement_count; i++) {
        m = &ctx->measurements[i];
        if (m->city < 0 || m->city >= NUM_CITIES) {
            continue;
        }
        city_stats = &ctx->city_stats[m->city];
        file_summary = &ctx->file_summaries[m->file_index];
        city_stats->total_records++;
        update_period(city_stats->period_start_key,
                      city_stats->period_end_key,
                      city_stats->period_start_br,
                      city_stats->period_end_br,
                      m);
        update_period(file_summary->period_start_key,
                      file_summary->period_end_key,
                      file_summary->period_start_br,
                      file_summary->period_end_br,
                      m);
        if (m->has_temperature) {
            metric_stats_add(&city_stats->temperature, m->temperature, m->timestamp_br);
        }
        if (m->has_humidity) {
            metric_stats_add(&city_stats->humidity, m->humidity, m->timestamp_br);
        }
        if (m->has_pressure) {
            metric_stats_add(&city_stats->pressure, m->pressure, m->timestamp_br);
        }
        if (m->has_battery) {
            if (city_stats->battery_count == 0 ||
                timestamp_is_before(m->timestamp_key, city_stats->battery_initial_key)) {
                city_stats->battery_initial = m->battery;
                safe_copy(city_stats->battery_initial_key,
                          sizeof(city_stats->battery_initial_key),
                          m->timestamp_key);
            }
            if (city_stats->battery_count == 0 ||
                timestamp_is_after(m->timestamp_key, city_stats->battery_final_key)) {
                city_stats->battery_final = m->battery;
                safe_copy(city_stats->battery_final_key,
                          sizeof(city_stats->battery_final_key),
                          m->timestamp_key);
            }
            city_stats->battery_count++;
        }
        if (m->has_sf) {
            city_add_sf(city_stats, m->spreading_factor);
        }
    }
    sort_sf_values(&ctx->city_stats[CITY_CAXIAS]);
    sort_sf_values(&ctx->city_stats[CITY_BENTO]);
}
static void format_metric_value(const MetricStats *stats,
                                int is_min,
                                char *out,
                                size_t out_size) {
    double value;
    if (out == NULL || out_size == 0) {
        return;
    }
    if (stats == NULL || stats->count == 0) {
        safe_copy(out, out_size, "N/I");
        return;
    }
    value = is_min ? stats->min_value : stats->max_value;
    snprintf(out, out_size, "%.2f", value);
}
static void format_metric_average(const MetricStats *stats, char *out, size_t out_size) {
    if (out == NULL || out_size == 0) {
        return;
    }
    if (stats == NULL || stats->count == 0) {
        safe_copy(out, out_size, "N/I");
        return;
    }
    snprintf(out, out_size, "%.2f", stats->sum / stats->count);
}
static const char *metric_time_or_missing(const MetricStats *stats, int is_min) {
    if (stats == NULL || stats->count == 0) {
        return "-";
    }
    return is_min ? stats->min_time : stats->max_time;
}
static void print_metric_line(const CityStats *city, const MetricStats *metric) {
    char min_text[MAX_SMALL_TEXT];
    char max_text[MAX_SMALL_TEXT];
    char avg_text[MAX_SMALL_TEXT];
    format_metric_value(metric, 1, min_text, sizeof(min_text));
    format_metric_value(metric, 0, max_text, sizeof(max_text));
    format_metric_average(metric, avg_text, sizeof(avg_text));
    printf("%-17s | %6s | %-21s | %6s | %-21s | %6s\n",
           city->city_name,
           min_text,
           metric_time_or_missing(metric, 1),
           max_text,
           metric_time_or_missing(metric, 0),
           avg_text);
}
static void print_battery_line(const CityStats *city) {
    if (city->battery_count == 0) {
        printf("%-17s | %11s | %9s | %10s\n", city->city_name, "N/I", "N/I", "N/I");
        return;
    }
    printf("%-17s | %11.2f | %9.2f | %10.2f\n",
           city->city_name,
           city->battery_initial,
           city->battery_final,
           city->battery_initial - city->battery_final);
}
static void build_sf_text(const CityStats *city, char *out, size_t out_size) {
    int i;
    char piece[32];
    size_t used;
    if (out == NULL || out_size == 0) {
        return;
    }
    if (city == NULL || city->sf_count == 0) {
        safe_copy(out, out_size, "Não informado");
        return;
    }
    out[0] = '\0';
    used = 0;
    for (i = 0; i < city->sf_count; i++) {
        snprintf(piece, sizeof(piece), "%sSF%d", (i == 0 ? "" : ", "), city->sf_values[i]);
        if (used + strlen(piece) + 1 >= out_size) {
            break;
        }
        strcat(out, piece);
        used += strlen(piece);
    }
}
static void print_sf_line(const CityStats *city) {
    char sf_text[MAX_TEXT];
    build_sf_text(city, sf_text, sizeof(sf_text));
    printf("%-17s | %s\n", city->city_name, sf_text);
}
static void imprimir_relatorio_final(ProgramContext *ctx, double elapsed_seconds) {
    int i;
    printf("============================================================\n");
    printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n\n");
    for (i = 0; i < NUM_INPUT_FILES; i++) {
        printf("Arquivo analisado: %s\n", ctx->file_summaries[i].filename);
        printf("Total de registros processados: %d\n", ctx->file_summaries[i].processed_records);
        printf("Período analisado: %s a %s\n\n",
               ctx->file_summaries[i].period_start_br,
               ctx->file_summaries[i].period_end_br);
    }
    printf("------------------------------------------------------------\n");
    printf("TEMPERATURA (°C)\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printf("-----------------------------------------------------------------------------------------------\n");
    print_metric_line(&ctx->city_stats[CITY_CAXIAS], &ctx->city_stats[CITY_CAXIAS].temperature);
    print_metric_line(&ctx->city_stats[CITY_BENTO], &ctx->city_stats[CITY_BENTO].temperature);
    printf("\n");
    printf("------------------------------------------------------------\n");
    printf("UMIDADE (%%)\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printf("-----------------------------------------------------------------------------------------------\n");
    print_metric_line(&ctx->city_stats[CITY_CAXIAS], &ctx->city_stats[CITY_CAXIAS].humidity);
    print_metric_line(&ctx->city_stats[CITY_BENTO], &ctx->city_stats[CITY_BENTO].humidity);
    printf("\n");
    printf("------------------------------------------------------------\n");
    printf("PRESSÃO ATMOSFÉRICA (hPa)\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printf("-----------------------------------------------------------------------------------------------\n");
    print_metric_line(&ctx->city_stats[CITY_CAXIAS], &ctx->city_stats[CITY_CAXIAS].pressure);
    print_metric_line(&ctx->city_stats[CITY_BENTO], &ctx->city_stats[CITY_BENTO].pressure);
    printf("\n");
    printf("------------------------------------------------------------\n");
    printf("BATERIA\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Inicial (V) | Final (V) | Consumo (V)\n");
    printf("------------------------------------------------------------\n");
    print_battery_line(&ctx->city_stats[CITY_CAXIAS]);
    print_battery_line(&ctx->city_stats[CITY_BENTO]);
    printf("\n");
    printf("------------------------------------------------------------\n");
    printf("SPREADING FACTORS UTILIZADOS\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | SF utilizados\n");
    printf("------------------------------------------------------------\n");
    print_sf_line(&ctx->city_stats[CITY_CAXIAS]);
    print_sf_line(&ctx->city_stats[CITY_BENTO]);
    printf("\n");
    printf("------------------------------------------------------------\n");
    printf("DESEMPENHO\n");
    printf("------------------------------------------------------------\n");
    printf("Tempo total de execução: %.2f segundos\n", elapsed_seconds);
    printf("Threads utilizadas: 3\n");
    printf(" - Thread 1: leitura dos dados\n");
    printf(" - Thread 2: cálculo das estatísticas\n");
    printf(" - Thread 3: registro de logs\n\n");
    printf("Arquivo de log gerado: %s\n\n", LOG_FILE_NAME);
    printf("============================================================\n");
    printf("Processamento finalizado com sucesso.\n");
    printf("============================================================\n");
}
static void program_context_init(ProgramContext *ctx, int argc, char **argv) {
    int i;
    memset(ctx, 0, sizeof(ProgramContext));
    if (argc >= 3) {
        safe_copy(ctx->input_files[0], sizeof(ctx->input_files[0]), argv[1]);
        safe_copy(ctx->input_files[1], sizeof(ctx->input_files[1]), argv[2]);
    } else {
        safe_copy(ctx->input_files[0], sizeof(ctx->input_files[0]), DEFAULT_FILE_CAXIAS);
        safe_copy(ctx->input_files[1], sizeof(ctx->input_files[1]), DEFAULT_FILE_BENTO);
    }
    for (i = 0; i < NUM_INPUT_FILES; i++) {
        safe_copy(ctx->file_summaries[i].filename,
                  sizeof(ctx->file_summaries[i].filename),
                  ctx->input_files[i]);
        safe_copy(ctx->file_summaries[i].period_start_br,
                  sizeof(ctx->file_summaries[i].period_start_br),
                  "-");
        safe_copy(ctx->file_summaries[i].period_end_br,
                  sizeof(ctx->file_summaries[i].period_end_br),
                  "-");
    }
    pthread_mutex_init(&ctx->data_mutex, NULL);
    pthread_cond_init(&ctx->data_ready, NULL);
    log_queue_init(&ctx->log_queue);
}
static void program_context_destroy(ProgramContext *ctx) {
    if (ctx == NULL) {
        return;
    }
    free(ctx->measurements);
    ctx->measurements = NULL;
    ctx->measurement_count = 0;
    ctx->measurement_capacity = 0;
    pthread_mutex_destroy(&ctx->data_mutex);
    pthread_cond_destroy(&ctx->data_ready);
    log_queue_destroy(&ctx->log_queue);
}
static double now_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}
static void *thread_leitura(void *arg) {
    ProgramContext *ctx = (ProgramContext *)arg;
    StringSet seen_keys;
    int i;
    if (ctx == NULL) {
        return NULL;
    }
    if (!string_set_init(&seen_keys, 4096)) {
        adicionar_log(ctx, "Falha ao inicializar estrutura de duplicatas.");
        pthread_mutex_lock(&ctx->data_mutex);
        ctx->reading_finished = 1;
        pthread_cond_signal(&ctx->data_ready);
        pthread_mutex_unlock(&ctx->data_mutex);
        return NULL;
    }
    for (i = 0; i < NUM_INPUT_FILES; i++) {
        process_json_file(ctx, i, &seen_keys);
    }
    string_set_free(&seen_keys);
    pthread_mutex_lock(&ctx->data_mutex);
    ctx->reading_finished = 1;
    pthread_cond_signal(&ctx->data_ready);
    pthread_mutex_unlock(&ctx->data_mutex);
    adicionar_log(ctx, "Thread de leitura finalizada.");
    return NULL;
}
static void *thread_estatisticas(void *arg) {
    ProgramContext *ctx = (ProgramContext *)arg;
    if (ctx == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&ctx->data_mutex);
    while (!ctx->reading_finished) {
        pthread_cond_wait(&ctx->data_ready, &ctx->data_mutex);
    }
    pthread_mutex_unlock(&ctx->data_mutex);
    adicionar_log(ctx, "Thread de estatisticas iniciando calculos.");
    calcular_estatisticas(ctx);
    adicionar_log(ctx, "Thread de estatisticas finalizada.");
    return NULL;
}
static void *thread_logs(void *arg) {
    ProgramContext *ctx = (ProgramContext *)arg;
    FILE *log_file;
    char *message;
    if (ctx == NULL) {
        return NULL;
    }
    log_file = fopen(LOG_FILE_NAME, "w");
    if (log_file == NULL) {
        fprintf(stderr, "Nao foi possivel abrir %s para escrita.\n", LOG_FILE_NAME);
    }
    while ((message = log_queue_pop(&ctx->log_queue)) != NULL) {
        if (log_file != NULL) {
            fprintf(log_file, "%s\n", message);
            fflush(log_file);
        }
        free(message);
    }
    if (log_file != NULL) {
        fclose(log_file);
    }
    return NULL;
}
int main(int argc, char **argv) {
    ProgramContext ctx;
    pthread_t tid_leitura;
    pthread_t tid_estatisticas;
    pthread_t tid_logs;
    double start_time;
    double end_time;
    int ok;
    setlocale(LC_ALL, "");
    start_time = now_seconds();
    program_context_init(&ctx, argc, argv);
    ok = pthread_create(&tid_logs, NULL, thread_logs, &ctx);
    if (ok != 0) {
        fprintf(stderr, "Erro ao criar thread de logs.\n");
        program_context_destroy(&ctx);
        return 1;
    }
    ok = pthread_create(&tid_estatisticas, NULL, thread_estatisticas, &ctx);
    if (ok != 0) {
        fprintf(stderr, "Erro ao criar thread de estatisticas.\n");
        log_queue_close(&ctx.log_queue);
        pthread_join(tid_logs, NULL);
        program_context_destroy(&ctx);
        return 1;
    }
    ok = pthread_create(&tid_leitura, NULL, thread_leitura, &ctx);
    if (ok != 0) {
        fprintf(stderr, "Erro ao criar thread de leitura.\n");
        pthread_mutex_lock(&ctx.data_mutex);
        ctx.reading_finished = 1;
        pthread_cond_signal(&ctx.data_ready);
        pthread_mutex_unlock(&ctx.data_mutex);
        pthread_join(tid_estatisticas, NULL);
        log_queue_close(&ctx.log_queue);
        pthread_join(tid_logs, NULL);
        program_context_destroy(&ctx);
        return 1;
    }
    pthread_join(tid_leitura, NULL);
    pthread_join(tid_estatisticas, NULL);
    end_time = now_seconds();
    adicionar_log(&ctx, "Tempo total medido pelo programa: %.2f segundos.", end_time - start_time);
    log_queue_close(&ctx.log_queue);
    pthread_join(tid_logs, NULL);
    imprimir_relatorio_final(&ctx, end_time - start_time);
    program_context_destroy(&ctx);
    return 0;
}
