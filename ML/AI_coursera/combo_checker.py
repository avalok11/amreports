#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
# from numpy import genfromtxt
import logistic_regression as lr
import matplotlib.pyplot as plt
import get_data as getd


def main():
    twice = 1  # количество перематров в выборке (увеличиваем и во сколько раз)
    learning_rate = 0.09
    num_iterations = 1000
    amount = 500  # количество выборок

    train_x, train_y = getd.main(20170901, twice=twice, amount=amount)
    train_x = train_x.values.astype(float)
    train_y = train_y.values.astype(float)
    None

    # print(train.shape)
    # train = np.delete(train, np.s_[670:], 0)
    # train = np.nan_to_num(train)
    train_x = (train_x - train_x.mean())/train_x.std()

    test_x, test_y = getd.main(20170801, twice=twice, amount=amount)
    test_x = test_x.values.astype(float)
    test_y = test_y.values.astype(float)
    None
    # print(test.shape)
    # test = np.delete(test, np.s_[670:], 0)
    test_x = np.nan_to_num(test_x)
    test_x = (test_x-test_x.mean())/test_x.std()
    test_y = test_y.reshape(test_y.shape[0], 1)
    #print(Ytest.shape)
    #test_y = np.delete(test_y, np.s_[670:], 0)

    d = lr.model(train_x.T, train_y.T, test_x.T, test_y.T, num_iterations=num_iterations, learning_rate=learning_rate,
                 print_cost=True)
    print('Iterration number:', num_iterations)
    print('Train shape is:', train_x.shape)
    print('Learning rate is: ', learning_rate)
    print('Amount of training and test sets is: ', amount)

    costs = np.squeeze(d['costs'])
    plt.plot(costs)
    plt.ylabel('cost')
    plt.xlabel('iterations (per hundreds)')
    plt.title("Learning rate =" + str(d["learning_rate"]))
    plt.show()


if __name__ == "__main__":
    main()
