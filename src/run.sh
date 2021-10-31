#!/bin/bash

#javac -cp controlPackage Master.java
#javac -cp workerPackage WorkerMapper.java
#javac -cp workerPackage WorkerReduce.java
#javac -cp utilsPackage IMapper.java
#javac -cp utilsPackage IReducer.java
#javac -cp tests MovieRating.java
#javac -cp tests ShoppingTrend.java
#javac -cp tests WordCount.java

javac controlPackage/Controller.java
java controlPackage.Controller

javac controlPackage/Controller2.java
java controlPackage.Controller2

javac controlPackage/Controller3.java
java controlPackage/Controller3
