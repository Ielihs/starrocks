// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/vectorized/json_functions.h"
#include "simdjson.h"

namespace starrocks::vectorized {

class JsonParser {
public:
    JsonParser() = default;
    virtual ~JsonParser() = default;
    // parse initiates the parser. The inner iterator would point to the first object to be returned.
    virtual Status parse(uint8_t* data, size_t len, size_t allocated) noexcept = 0;
    // get returns the object pointed by the inner iterator.
    virtual Status get_current(simdjson::ondemand::object* row) noexcept = 0;
    // next forwards the inner iterator.
    virtual Status advance() noexcept = 0;
};

// JsonDocumentStreamParser parse json in document stream (ndjson).
// eg:
// input: {"key":1} {"key":2}
class JsonDocumentStreamParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    // data is parsed as a document stream.

    // iterator context for document stream.
    simdjson::ondemand::document_stream _doc_stream;
    simdjson::ondemand::document_stream::iterator _doc_stream_itr;

    // Iterator (value, object, array, etc) in simdjson could be only parsed once.
    // If we want to access iterator twice, a call of rewind/reset is needed.
    // get_current would access many iterators, which are hard to reset and get the object.
    // Hance, we keep the object returned in _curr and reset it when necessary.

    // _curr is the object returned by get_current.
    simdjson::ondemand::object _curr;
    // _curr_ready denotes whether the _curr has been parsed.
    bool _curr_ready = false;
};

// JsonArrayParser parse json in json array
// eg:
// input: [{"key": 1}, {"key": 2}].
class JsonArrayParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    // data is parsed as a document in array type.
    simdjson::ondemand::document _doc;

    // iterator context for array.
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;

    // Iterator (value, object, array, etc) in simdjson could be only parsed once.
    // If we want to access iterator twice, a call of rewind/reset is needed.
    // get_current would access many iterators, which are hard to reset and get the object.
    // Hance, we keep the object returned in _curr and reset it when necessary.

    // _curr is the object returned by get_current.
    simdjson::ondemand::object _curr;
    // _curr_ready denotes whether the _curr has been parsed.
    bool _curr_ready = false;
};

// JsonDocumentStreamParserWithRoot parse json in document stream (ndjson) with json root.
// eg:
// input: {"data": {"key":1}} {"data": {"key":2}}
// json root: $.data
class JsonDocumentStreamParserWithRoot : public JsonDocumentStreamParser {
public:
    JsonDocumentStreamParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;

    // Iterator (value, object, array, etc) in simdjson could be only parsed once.
    // If we want to access iterator twice, a call of rewind/reset is needed.
    // get_current would access many iterators, which are hard to reset and get the object.
    // Hance, we keep the object returned in _curr and reset it when necessary.

    // _curr is the object returned by get_current.
    simdjson::ondemand::object _curr;
    // _curr_ready denotes whether the _curr has been parsed.
    bool _curr_ready = false;
};

// JsonArrayParserWithRoot parse json in json array with json root.
// eg:
// input: [{"data": {"key":1}}, {"data": {"key":2}}]
// json root: $.data
class JsonArrayParserWithRoot : public JsonArrayParser {
public:
    JsonArrayParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;

    // Iterator (value, object, array, etc) in simdjson could be only parsed once.
    // If we want to access iterator twice, a call of rewind/reset is needed.
    // get_current would access many iterators, which are hard to reset and get the object.
    // Hance, we keep the object returned in _curr and reset it when necessary.

    // _curr is the object returned by get_current.
    simdjson::ondemand::object _curr;
    // _curr_ready denotes whether the _curr has been parsed.
    bool _curr_ready = false;
};

// ExpandedJsonDocumentStreamParserWithRoot parses json in document stream (ndjson) with json root, and expands the array under json root.
// eg:
// input: {"data": [{"key":1}, {"key":2}]} {"data": [{"key":3}, {"key":4}]}
// json root: $.data
class ExpandedJsonDocumentStreamParserWithRoot : public JsonDocumentStreamParser {
public:
    ExpandedJsonDocumentStreamParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;

    // data is parsed as a document stream, in which every document is an object and has an array under json root.

    // _curr_row is the current document in document stream.
    simdjson::ondemand::object _curr_row;

    // iterator context for array under json root.
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;

    // Iterator (value, object, array, etc) in simdjson could be only parsed once.
    // If we want to access iterator twice, a call of rewind/reset is needed.
    // get_current would access many iterators, which are hard to reset and get the object.
    // Hance, we keep the object returned in _curr and reset it when necessary.

    // _curr is the object returned by get_current.
    simdjson::ondemand::object _curr;
    // _curr_ready denotes whether the _curr has been parsed.
    bool _curr_ready = false;
};

// ExpandedJsonArrayParserWithRoot parses json in json array with json root, and expands the array under json root.
// eg:
// input: [{"data": [{"key":1}, {"key":2}]}, {"data": [{"key":3}, {"key":4}]}]
// json root: $.data
class ExpandedJsonArrayParserWithRoot : public JsonArrayParser {
public:
    ExpandedJsonArrayParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;

    // data is parsed as an array, in which every document is an object and has an array under json root.

    // _curr_row is the current document in array.
    simdjson::ondemand::object _curr_row;

    // iterator context for array under json root.
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;

    // Iterator (value, object, array, etc) in simdjson could be only parsed once.
    // If we want to access iterator twice, a call of rewind/reset is needed.
    // get_current would access many iterators, which are hard to reset and get the object.
    // Hance, we keep the object returned in _curr and reset it when necessary.

    // _curr is the object returned by get_current.
    simdjson::ondemand::object _curr;
    // _curr_ready denotes whether the _curr has been parsed.
    bool _curr_ready = false;
};

} // namespace starrocks::vectorized