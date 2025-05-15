#include <iostream>
#include <ctime>
#include <iomanip>
#include "cache/lorc/logger.h"

namespace ROCKSDB_NAMESPACE {
namespace lorc {

Logger::Level Logger::currentLevel = Logger::DEBUG;
bool Logger::enabled = true;

void Logger::setLevel(Level level) {
    currentLevel = level;
}

void Logger::setEnabled(bool enable) {
    enabled = enable;
}

std::string Logger::getLevelString(Level level) {
    switch (level) {
        case DEBUG: return "DEBUG";
        case INFO:  return "INFO";
        case WARN:  return "WARN";
        case ERROR: return "ERROR";
        default:    return "UNKNOWN";
    }
}

void Logger::log(Level level, const std::string& message) {
    if (!enabled || level < currentLevel) return;
    
    auto now = std::time(nullptr);
    auto tm = *std::localtime(&now);
    
    std::cout << "[" << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "] "
              << "[" << getLevelString(level) << "] "
              << message << std::endl;
}

void Logger::debug(const std::string& message) {
    log(DEBUG, message);
}

void Logger::info(const std::string& message) {
    log(INFO, message);
}

void Logger::warn(const std::string& message) {
    log(WARN, message);
}

void Logger::error(const std::string& message) {
    log(ERROR, message);
}

}  // namespace lorc
}  // namespace ROCKSDB_NAMESPACE
