#pragma once
#include <string>

class Logger {
public:
    enum Level {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
    };

    static void setLevel(Level level);
    static void setEnabled(bool enabled);
    static void log(Level level, const std::string& message);
    static void debug(const std::string& message);
    static void info(const std::string& message);
    static void warn(const std::string& message);
    static void error(const std::string& message);

    static Level currentLevel;
    static bool enabled;
    static std::string getLevelString(Level level);
};
