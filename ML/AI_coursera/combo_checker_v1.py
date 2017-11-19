#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
from numpy import genfromtxt
import logistic_regression as lr
import matplotlib.pyplot as plt

def main():
    train = genfromtxt(r'C:\Users\aleksey.yarkov\OneDrive - AmRest\_Projects\AI_ML\COMBO\train3.csv', delimiter=';')
    #print(train.shape)
    train = np.delete(train, np.s_[670:], 0)
    train = np.nan_to_num(train)
    train = (train - train.mean())/train.std()
    Ytrain = genfromtxt(r'C:\Users\aleksey.yarkov\OneDrive - AmRest\_Projects\AI_ML\COMBO\Ytrain3.csv', delimiter=';')
    Ytrain = Ytrain.reshape(Ytrain.shape[0], 1)
    #print(Ytrain.shape)
    Ytrain = np.delete(Ytrain, np.s_[670:], 0)

    test = genfromtxt(r'C:\Users\aleksey.yarkov\OneDrive - AmRest\_Projects\AI_ML\COMBO\test3.csv', delimiter=';')
    #print(test.shape)
    test = np.delete(test, np.s_[670:], 0)
    test = np.nan_to_num(test)
    test = (test-test.mean())/test.std()
    Ytest = genfromtxt(r'C:\Users\aleksey.yarkov\OneDrive - AmRest\_Projects\AI_ML\COMBO\Ytest3.csv', delimiter=';')
    Ytest = Ytest.reshape(Ytest.shape[0], 1)
    #print(Ytest.shape)
    Ytest = np.delete(Ytest, np.s_[670:], 0)

    #w, b = lr.initialize_with_zeros(11)
    d = lr.model(train.T, Ytrain.T, test.T, Ytest.T, num_iterations=10000, learning_rate=1, print_cost=True)
    print('Iterration number:', 10000)

    costs = np.squeeze(d['costs'])
    plt.plot(costs)
    plt.ylabel('cost')
    plt.xlabel('iterations (per hundreds)')
    plt.title("Learning rate =" + str(d["learning_rate"]))
    plt.show()








if __name__ == "__main__":
    main()
