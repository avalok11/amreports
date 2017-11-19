#!/usr/local/bin/python3.5
# -*- coding: utf-8 -*-

import numpy as np
# from numpy import genfromtxt
import neural_network as nn
import matplotlib.pyplot as plt
import get_data as getd
import sys


def main():
    filename = "logs_ml.log"
    # log_file = open(filename, "w")
    # sys.stdout = log_file
    print("Start logs.")

    twice = 0  # количество пераметров в выборке (увеличиваем twice раз)
    learning_rate = 1.2
    num_iterations = 5000
    amount = 100  # количество выборок
    n_h = 19  # размер скрытого слоя

    # подготовка тренировочного сета
    train_x, train_y = getd.main(20170901, twice=twice, amount=amount)
    train_x = train_x.values.astype(float)
    train_y = train_y.values.astype(float)
    None
    train_x = (train_x - train_x.mean())/train_x.std()
    train_y = train_y.reshape(train_y.shape[0], 1)

    # подготовка тестового сета
    test_x, test_y = getd.main(20170801, twice=twice, amount=amount)
    test_x = test_x.values.astype(float)
    test_y = test_y.values.astype(float)
    None
    test_x = np.nan_to_num(test_x)
    test_x = (test_x-test_x.mean())/test_x.std()
    test_y = test_y.reshape(test_y.shape[0], 1)

    X = train_x.T
    Y = train_y.T
    Xt = test_x.T
    Yt = test_y.T

    hidden_layer_sizes = [50, 500]
    learning_rate_numbers = [1.2, 5.2]
    for i, n_h in enumerate(hidden_layer_sizes):
        for learning_rate in learning_rate_numbers:
            print("\n==============")
            print("Hidden layers: ", n_h)
            print("Learning rate:", learning_rate)
            print('Iterration number:', num_iterations)
            print('Train shape is:', train_x.shape)
            print('Amount of training and test sets is: ', amount*2)
            parameters = nn.nn_model(X, Y, n_h, num_iterations=num_iterations,  print_cost=True,
                                     learning_rate=learning_rate)
            predictions = nn.predict(parameters, Xt)
            accuracy = float((np.dot(Yt, predictions.T) + np.dot(1 - Yt, 1 - predictions.T)) / float(Yt.size) * 100)
            print("Accuracy for {} hidden units: {} %".format(n_h, accuracy))

    #parameters = nn.nn_model(X, Y, n_h, num_iterations=num_iterations, print_cost=True, learning_rate=learning_rate)

    #X = test_x.T
    #Y = test_y.T
    #predictions = nn.predict(parameters, X)
    #print('Accuracy: %d' % float(
    #    (np.dot(Y, predictions.T) + np.dot(1 - Y, 1 - predictions.T)) / float(Y.size) * 100) + '%')

    print('Iterration number:', num_iterations)
    print('Train shape is:', train_x.shape)
    print('Learning rate is: ', learning_rate)
    print('Amount of training and test sets is: ', amount)


if __name__ == "__main__":
    main()
