<a name="br1"></a> 

**Word Prediction**

**Authors**

•

•

•

Tamir Nizri (211621552)

Eyal Segev (315144717)

Lior Hagay (314872367)

**Project Overview**

This project generates a knowledge-base for a Hebrew word-prediction system

using the Google 3-Gram,2-Gram and 1-Gram Hebrew datasets on Amazon

Elastic MapReduce (EMR). The system calculates the conditional probability of

each word in a trigram based on the dataset, producing a knowledge base that

indicates the probability of possible next words for each pair of words.

The primary goal is to develop an eﬀicient and scalable system capable of

predicting the likelihood of word sequences, enhancing the functionality of

language modeling applications and ensuring cost-eﬀectiveness in handling

large datasets.

Input:

The project utilizes the Hebrew 1-Gram, 2-Gram, and 3-Gram datasets available

at these links:

1\. s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data

2\. s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data

3\. s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

These datasets contain sequences of words with their corresponding usage

frequencies across a corpus of literature.

Output:

The system produces a sorted list of trigrams along with their conditional

probabilities. This list is organized lexicographically by the ﬁrst two words of the

trigram and in descending order by the probability of the third word. The output is

stored on S3, ensuring scalability and accessibility.

\* Location in S3: s3://hashem-itbarach/output

![](data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/2wBDAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/wAARCAAEAAQDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAr/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFAEBAAAAAAAAAAAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/AL+AAf/Z)

<a name="br2"></a> 

**How to Run the Project**

**(\*) –** Regarding steps 1+2: The JAR ﬁles should already be prepared and stored in

S3, in this location: s3://hashem-itbarach/jars

1\. **Prepare the JAR Files (\*):**

o

Use Maven to package the project into separate JARs for each step

and the application.

o

o

Command: mvn clean package

Modify the pom.xml to specify the main class for the JAR you are

creating.

2\. **Upload the JARs (\*):**

Ensure that all JAR ﬁles for the steps are uploaded to the correct

S3 paths as needed.

3\. **Execute the Application:**

o

o

o

Run App.java locally to run the EMR jobs.

Command: mvn exec:java -Dexec.mainClass="App"



<a name="br3"></a> 

**Project Description**

**Step 1 - N-Gram Mapping and Reduction**

•

•

**Partitioner:** Sorts n-grams in lexicographic order by the ﬁrst word.

**Mapper:** Maps each line of the ﬁle to reducers in the format <key=n-gram,

value=matchCount>.

•

**Reducer:** Processes pairs in the format <key=n-gram,

value=matchCount> and outputs key-value pairs based on the number of

words in the n-gram:

o

o

o

For 1-gram (w1): (w1, "w1:count")

For 2-gram (w1 w2): (w1 w2, "w1:count w1 w2: count")

For 3-gram (w1 w2 w3): (w1 w2 w3, "w1:count w1 w2: count w1 w2

w3:count")

**Step 2 - Intermediate N-Gram Processing**

•

•

**Partitioner:** Maintains lexicographic order by the ﬁrst word of the n-gram.

**Mapper:** Maps lines to reducers based on the n-gram word count:

o

o

o

Outputs like: <key=w1, value="w1:count"> for 1-gram,

<key=w1 w2, value="w1:count w1 w2: count"> for 2-gram,

<key=w1 w2 w3, value="w1:count w1 w2: count w1 w2 w3:count">

gets redirected to <key=w2 w3, value="w1:count w1 w2: count w1

w2 w3:count"> for 3-gram.

•

**Reducer:** Joins and reduces pairs according to the number of words in

the n-gram and outputs:

o

o

For 1-gram: (w1, "w1:count")

Joins (w2 w3, str with 2 values) with (w2 w3, str with 3 values) to

output (w3, str with 5 values).

**Step 3 - Final N-Gram Aggregation**

•

•

•

**Partitioner:** Sorts by the ﬁrst word of the n-gram.

**Mapper:** Directly passes all key-value pairs as they are.

**Reducer:** Joins entries to reconstruct the original 3-gram and its

conditional probabilities by analyzing the number of values:



<a name="br4"></a> 

o

Joins (w3, str with 1 value) with (w3, str with 5 values) to form (w1

w2 w3, str with 6 values).

**Step 4 - Probability Calculation and Sorting**

•

•

•

**Partitioner:** Sorts based on the ﬁrst two words of the 3-gram.

**Mapper:** Calculates the conditional probability for each 3-gram.

**Reducer:** Outputs the 3-gram and probability pairs directly without

modiﬁcation.

•

**Comparator:** Lexicographically sorts 3-grams by the ﬁrst two words and,

for ties, by descending probability of the third word.

**System Requirements**

•

•

•

Amazon EMR

Maven for Java project management

Access to AWS S3 for storage of intermediate and ﬁnal outputs


