//
// Created by yuly on 06.04.23. add --sizter 1.17.25
//

#include "vector/DateColumnVector.h"
#include <iostream>
#include <sstream>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <algorithm>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		this->dates = nullptr;
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
    isNull[elementNum] = false;
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::add(std::string &value) {
	std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
        std::istringstream ss(value);
    	int year, month, day;
    	char dash1, dash2;  // 用来读取 '-' 分隔符

    // 解析输入的日期字符串
    	if (!(ss >> year >> dash1 >> month >> dash2 >> day) || dash1 != '-' || dash2 != '-') {
        	throw std::invalid_argument("Invalid date format. Expected: YYYY-M-D");
    	}

    // 创建一个日期对象
    	boost::gregorian::date d(year, month, day);

    // 计算与1970-1-1之间的天数差
    	boost::gregorian::date epoch(1970, 1, 1);
    	int diff = (d - epoch).days();

  		add(diff);
    }
}

void DateColumnVector::add(bool value) {
  	add(value ? 1 : 0);
}

void DateColumnVector::add(int value) {
  	if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    set(writeIndex ++, value);
}

void DateColumnVector::add(int64_t value) {
	if (writeIndex >= length) {
		ensureSize(writeIndex * 2, true);
	}

	set(writeIndex ++, value);
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        int *oldVector = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                      	size * sizeof(int32_t));
       	if (preserveData) {
            std::copy(oldVector, oldVector + length, dates);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof(int) * (size - length);
        resize(size);
    }
}
