# Prediction of Drug Binding Affinity of Protein Using Spark ML

Healthcare, Pharmacy and Biological Big Data have entered the digital era. Computational drug discovery is an effective strategy for accelerating and economizing drug discovery and development process.Filtering large compound libraries into smaller sets of predicted active compounds using computational models that can be tested experimentally for accurate match is the most common practice.
In my project I want to predict the likelihood of binding between sample drug compounds and target protein.

 

# Dataset Description

The data used in this project is filtered from  [ChEMBLE Database](https://www.ebi.ac.uk/chembl/target_report_card/CHEMBL4040/) website. 

For the project I have chosen ‘MAP kinase ERK2’ as my target protein.

There are 5 features and 4643 observations.

Data Dictionary
- NumHDonors : No more than 5 hydrogen bond donors (the total number of nitrogen–hydrogen and oxygen–hydrogen bonds)
- NumHAcceptors: No more than 10 hydrogen bond acceptors (all nitrogen or oxygen atoms)
- MW: A molecular mass less than 500 daltons.
- LogP : An octanol-water partition coefficient[10] (log P) that does not exceed 5
- bioactivity_class: Is the drug compound is active or inactive for target protein.

# Problem Statement

To Predict Drug Binding Affinity of Protein.


# Results

 I have compared the areUnderROC for Logistic Regression, Decision Tree and Random Forest models but there is not much difference. Random Forest is giving 1 value but it is probably overfitting to the dataset. Currently my data is very small, that's why decision trees and random forests are fast. If we try it on large scale DT and RF will be very computationally costly so Logistic Regression will be the best choice. We can see Logistic Regression is giving better performance 0.9984. Also the accuracy of the Logistic Regression model is 96% which is a pretty good score. 

# Future Scope

- Using additional features other than the rule of five.
- Using Multiclass Classifier
- Targeting more Proteins to find drug compounds. 

# User Guide

Files :

- bioactivity_preprocessed_data.csv - Dataset
- proposal.pdf - Project proposal slides.
- progress.pdf - Project progress report.
- Data603_final_project.ipynb - Jupyter Notebook containing modeling.
- project/presentation.pdf - Project overview 
