//
// Created by drobison on 6/22/21.
//

#include <memory>

#ifndef TASKFLOW_BATCH_HPP
#define TASKFLOW_BATCH_HPP
template<class DataType>
class Batch {
public:
    // Maybe other info
    unsigned int batch_size;
    std::shared_ptr<DataType[]> batch;

    Batch() : batch_size{0}, batch{nullptr} {};

    Batch(unsigned int batch_size) : batch_size{batch_size} {
        batch = std::shared_ptr<DataType[]>(new DataType[batch_size]);
    };
};
#endif //TASKFLOW_BATCH_HPP
