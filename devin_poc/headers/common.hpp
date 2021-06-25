//
// Created by drobison on 6/24/21.
//
#include <memory>

#include <nlohmann/json.hpp>

#include <boost/fiber/all.hpp>
#include <boost/variant.hpp>

#include <blockingconcurrentqueue.h>

#ifndef TASKFLOW_COMMON_HPP
#define TASKFLOW_COMMON_HPP
template<typename T>
constexpr auto type_name() noexcept {
std::string_view name = "Error: unsupported compiler", prefix, suffix;
#ifdef __clang__
name = __PRETTY_FUNCTION__;
              prefix = "auto type_name() [T = ";
              suffix = "]";
#elif defined(__GNUC__)
name = __PRETTY_FUNCTION__;
              prefix = "constexpr auto type_name() [with T = ";
              suffix = "]";
#elif defined(_MSC_VER)
name = __FUNCSIG__;
              prefix = "auto __cdecl type_name<";
              suffix = ">(void) noexcept";
#endif
name.remove_prefix(prefix.size());
name.remove_suffix(suffix.size());

return name;
}

struct sentinel {};

typedef boost::variant<
    std::shared_ptr<std::string>,
    std::shared_ptr<nlohmann::json>,
    std::shared_ptr<int>,
    std::shared_ptr<double>,
    std::shared_ptr<sentinel>
> PrimitiveVariant;

typedef boost::variant<
    PrimitiveVariant,
    std::vector<PrimitiveVariant>
> DataVariant;

typedef moodycamel::BlockingConcurrentQueue < DataVariant> Edge;
typedef std::shared_ptr<moodycamel::BlockingConcurrentQueue < DataVariant>> EdgePtr;

#endif //TASKFLOW_COMMON_HPP
