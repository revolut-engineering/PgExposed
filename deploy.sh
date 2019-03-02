#!/bin/bash

git checkout $TRAVIS_BRANCH
git reset --hard $TRAVIS_COMMIT
git config --local user.name "Travis CI"
git config --local user.email "travis@travis-ci.com"

echo "asked to deploy: $TRAVIS_COMMIT"
git status
git log --oneline -n 1

./gradlew release -Prelease.useAutomaticVersion=true
