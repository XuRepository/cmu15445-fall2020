//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    //构造器
    rows = r;
    cols = c;
    linear = new T[c * r]; // 存储二维数组
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() {
    //析构函数，释放存储matrix的数组
    delete linear;
  }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    //子类构造器调用父类的构造器，如果不指定构造器，就是默认调用父类的无参构造器
    data_ = new T * [r](); //T*代表了一个数组，也就是一行；所以T*[r]就是一个r行的二维数组

    for (int i = 0; i < r; ++i) {
      data_[i] = this->linear+i*c; //linear分割为r行！
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override {data_[i][j] = val;}

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    for (int i = 0; i < this->GetRows(); ++i) {
      this->data_[i] = arr+i*this->GetColumns();
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override {
    delete data_;
    //不需要在释放linear了，linear和data用的同一片内存空间
  }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    //1,判断俩matrix是不是一样形状
    int row1 = mat1->GetRows();
    int row2 = mat2->GetRows();
    int col1 = mat1->GetColumns();
    int col2 = mat2->GetColumns();
    if (row1!=row2 || col1!=col2){//形状不同
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> res_mat(new RowMatrix<T>(row1,col1));
    for (int i = 0; i < row1; ++i) {
      for (int j = 0; j < col1; ++j) {
        res_mat->SetElem(i,j,mat1->GetElem(i,j) + mat2->GetElem(i,j));
      }
    }
    return res_mat;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    //矩阵相乘，要求matrix1的列数和matrix2的行数一样
    int row1 = mat1->GetRows();
    int row2 = mat2->GetRows();
    int col1 = mat1->GetColumns();
    int col2 = mat2->GetColumns();

    if (row2!=col1){
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> res_mat(new RowMatrix<T>(row1,col2));
    for (int i = 0; i < row1; ++i) {
      for (int j = 0; j < col2; ++j) {
        int value = 0;
        for (int k = 0; k < col1; ++k) {
          value+=mat1->GetElem(i,k)*mat2->GetElem(k,j);
        }
        res_mat->SetElem(i,j,value);
      }
    }
    return res_mat;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    std::unique_ptr<RowMatrix<T>> tmp_mat = MultiplyMatrices(matA,matB);
    if (tmp_mat == nullptr){
      return tmp_mat;
    }
    return AddMatrices(tmp_mat,matC);
  }
};
}  // namespace bustub
