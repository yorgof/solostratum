/* Stand-in for the ESP-IDF header: logging is discarded. */
#ifndef SHIM_ESP_LOG_H
#define SHIM_ESP_LOG_H
#define ESP_LOGE(tag, ...) ((void)(tag))
#define ESP_LOGW(tag, ...) ((void)(tag))
#define ESP_LOGI(tag, ...) ((void)(tag))
#define ESP_LOGD(tag, ...) ((void)(tag))
#define ESP_LOGV(tag, ...) ((void)(tag))
#endif
