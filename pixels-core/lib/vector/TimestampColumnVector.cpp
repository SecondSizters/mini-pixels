//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <iostream>
#include <sstream>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <algorithm>


TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
    isNull[elementNum] = false;
}

void TimestampColumnVector::add(bool value) {
    add(value ? 1 : 0);
}

void TimestampColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    set(writeIndex ++, value);
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    set(writeIndex ++, value);
}

void TimestampColumnVector::add(std::string &value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
        std::istringstream ss(value);
        int year, month, day;
        int hour, minute, second;
        char dash1, dash2, space, colon1, colon2;  // 用来读取分隔符

        // 解析输入的时间戳字符串
        if (!(ss >> year >> dash1 >> month >> dash2 >> day >> space >> hour >> colon1 >> minute >> colon2 >> second) ||
             dash1 != '-' || dash2 != '-' || space != ' ' || colon1 != ':' || colon2 != ':') {
            throw std::invalid_argument("Invalid timestamp format. Expected: YYYY-M-D H:M:S");
             }

        // 创建一个ptime对象
        boost::posix_time::ptime pt(boost::gregorian::date(year, month, day),
                                    boost::posix_time::time_duration(hour, minute, second));

        // 计算与1970-1-1之间的微秒差
        boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
        boost::posix_time::time_duration diff = pt - epoch;

        // 将时间差转换为微秒
        long microseconds = diff.total_microseconds();

        add(microseconds);
    }
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldVector = times;
        posix_memalign(reinterpret_cast<void **>(&times), 32,
                       size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, times);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof(long) * (size - length);
        resize(size);
    }
}

