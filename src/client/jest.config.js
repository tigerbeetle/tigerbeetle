'use strict'

module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  clearMocks: true,
  testPathIgnorePatterns: ['dist'],
  resetMocks: true
}
