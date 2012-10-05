REPORTER = dot
test:
	@NODE_ENV=test ./node_modules/.bin/mocha -b --reporter $(REPORTER)

lib-cov:
	jscoverage lib lib-cov

test-cov:	lib-cov
	@PROZESS_COVERAGE=1 $(MAKE) test REPORTER=html-cov > coverage.html
	rm -rf lib-cov

.PHONY: test 
