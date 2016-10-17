# spark_project
Spam filter with Apache Spark

We will explore the Ling-Spam email dataset. The archive “ling-spam.zip” from the Jalon website contains two directories, spam/ and ham/, containing the spam and legitimate emails, respectively. The dataset contains 2412 ham emails and 481 spam emails, all of which were received by a mailing list on linguistics. We want to extract the words that are most informative of whether an email is spam or ham. This extraction is called the spam filter.

The first steps in any natural language processing workflow are to remove stop words and lemmatization. Removing stop words involves filtering very common words such as the, this and so on. Lemmatization involves replacing different forms of the same word with a canonical form: both colors and color would be mapped to color, and organize, organizing and organizes would be mapped to organize. Removing stop words and lemmatization is very challenging, and beyond the scope of this project. The Ling-Spam e-mail dataset has been already cleaned and lemmatized.

When we do build the spam filter, we will use the presence of a particular word in an email as the feature for our model. We will use a bag-of-words approach: we consider which words appear in an email, but not the word order. Intuitively, some words will be more important than others when deciding whether an email is spam. For instance, an email that contains language is likely to be ham, since the mailing list was for linguistics discussions, and language is a word unlikely to be used by spammers. Conversely, words which are common to both message types, for instance hello, are unlikely to be much use.

One way of quantifying the importance of a word in determining whether a message is spam is the Mutual Information (MI). The mutual information is the gain in information about whether a message is ham or spam if we know that it contains a particular word. For instance, the presence of language in a particular email is very informative as to whether that email is spam or ham. Similarly, the presence of the word dollar is informative since it appears often in spam messages and only infrequently in ham messages. By contrast, the presence of the word morning is uninformative, since it is approximately equally common in both spam and ham messages. Consider a particular word w in an email. The email can belong to two classes: spam or ham. The word w can occur in the email or not. The mutual information MI of the word w whether that email is spam or ham is then defined by:

𝑀𝐼(𝑤)=Σ𝑃(𝑜𝑐𝑐𝑢𝑟𝑠,𝑐𝑙𝑎𝑠𝑠) log2(𝑃(𝑜𝑐𝑐𝑢𝑟𝑠,𝑐𝑙𝑎𝑠𝑠)𝑃(𝑜𝑐𝑐𝑢𝑟𝑠)𝑃(𝑐𝑙𝑎𝑠𝑠))𝑜𝑐𝑐𝑢𝑟𝑠 ∈ {𝑡𝑟𝑢𝑒,𝑓𝑎𝑙𝑠𝑒}𝑐𝑙𝑎𝑠𝑠 ∈ {𝑠𝑝𝑎𝑚,ℎ𝑎𝑚}

##Materials:
1. The zip file “ling-spam.zip”.
2. The file “build.sbt” for using sbt.
3. The template “template.scala” file

