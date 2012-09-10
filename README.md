rHpcc
=====

Interface between HPCC and R rHpcc is an R package providing an Interface between R and HPCC. 
Familiarity with ECL (Enterprise Control Language) is a must to use this package. HPCC is a massive parallel-processing computing platform that solves Big Data problems. 
ECL is the Enterprise Control Language designed specifically for huge data projects using the HPCC platform. 
Its extreme scalability comes from a design that allows you to leverage every query you create for re-use in subsequent queries as needed. 
To do this, ECL takes a dictionary approach to building queries wherein each ECL definition defines an Attribute. 
Each previously defined Attribute can then be used in succeeding ECL Attribute definitions as the language extends itself as you use it.

Depends: R (>= 2.11.0), methods, RCurl, XML.
URL: http://hpccsystems.com