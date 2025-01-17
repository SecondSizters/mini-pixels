//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include <algorithm>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding): ColumnVector(len, encoding) {
	// decimal column vector has no encoding so we don't allocate memory to this->vector
	this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;
    memoryUsage += (uint64_t) sizeof(uint64_t) * len;
}

void DecimalColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
		vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}

void DecimalColumnVector::add(std::string &value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
     	std::size_t dotPos = value.find('.');

    	if (dotPos == std::string::npos) {
        // 如果没有找到小数点，则直接尝试转换整个字符串
        	add(std::stol(value));
    	}

    	// 构建一个新的无小数点的字符串
    	std::string intPart = value.substr(0, dotPos);
    	std::string fracPart = value.substr(dotPos + 1);

    	// 确保分数部分正好两位
    	while (fracPart.length() < scale) {
        	fracPart += '0';  // 补零直到两位
    	}

    // 合并整数部分和调整后的分数部分
    	std::string fullIntStr = intPart + fracPart;

        add(std::stol(fullIntStr));
    }
}

void DecimalColumnVector::add(int64_t value) {
  	if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;
}

void DecimalColumnVector::add(int value) {
  	if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;
}

void DecimalColumnVector::add(bool value) {
  	add(value ? 1 : 0);
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
    	long *oldVector = vector;
    	posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       size * sizeof(int64_t));
    	if (preserveData) {
        	std::copy(oldVector, oldVector + length, vector);
   		}
        delete[] oldVector;
        memoryUsage += (long) sizeof(long) * (size - length);
        resize(size);
    }
}