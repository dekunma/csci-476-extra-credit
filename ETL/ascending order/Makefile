# usage: $ make ==> compile and run the MapReduce job with files under current directory
#		 $ make clean ==> delete .class files in current directory, and the output directory in HDFS
#		 $ make result ==> cat the output file in HDFS

# change these variables---------------------------------
PROJECT_NAME := Clean
USER_NAME := dm4524
INPUT_FILE_NAME := owid-covid-data.csv
INPUT_FILE := /user/$(USER_NAME)/input/$(INPUT_FILE_NAME)
OUTPUT_DIR := /user/$(USER_NAME)/final-project/monday/cleaning
#--------------------------------------------------------

JAVA_FILES := $(PROJECT_NAME)Mapper.java $(PROJECT_NAME)Reducer.java $(PROJECT_NAME).java
JAVA_CLASSES := $(PROJECT_NAME).class $(PROJECT_NAME)Reducer.class $(PROJECT_NAME)Mapper.class

all: hadoop

build: $(JAVA_FILES)

$(PROJECT_NAME).jar: $(JAVA_CLASSES)
	jar cvf $(PROJECT_NAME).jar ./*.class

hadoop: $(PROJECT_NAME).jar
	hadoop jar ./$(PROJECT_NAME).jar $(PROJECT_NAME) $(INPUT_FILE) $(OUTPUT_DIR)

clean:
	rm *.class
	hadoop fs -rm -r $(OUTPUT_DIR)

result: 
	hadoop fs -cat $(OUTPUT_DIR)/part-r-00000

%.class : %.java
	javac -classpath `yarn classpath` -d . *.java

get:
	hadoop dfs -get $(OUTPUT_DIR)/part-r-00000 .