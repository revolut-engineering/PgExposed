#!/bin/bash

git checkout $TRAVIS_BRANCH
git reset --hard $TRAVIS_COMMIT
git config --local user.name "Jacek Spolnik"
git config --local user.email "jacek.spolnik@gmail.com"

echo "asked to deploy: $TRAVIS_COMMIT"
git status
git log --oneline -n 1

./gradlew release -Prelease.useAutomaticVersion=true
