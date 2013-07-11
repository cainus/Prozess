REPORTER = dot
test: npm-install
	@NODE_ENV=test ./node_modules/.bin/mocha --reporter $(REPORTER)

lib-cov:
	jscoverage lib lib-cov

test-cov:	lib-cov
	@PROZESS_COVERAGE=1 $(MAKE) test REPORTER=html-cov > coverage.html
	rm -rf lib-cov

test-coveralls:	lib-cov
	echo TRAVIS_JOB_ID $(TRAVIS_JOB_ID)
	@PROZESS_COVERAGE=1 $(MAKE) test REPORTER=json-cov 2> /dev/null | ./node_modules/coveralls/bin/coveralls.js
	rm -rf lib-cov

npm-install:
	npm install

.PHONY: test 
