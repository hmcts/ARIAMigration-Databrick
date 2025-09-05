import importlib.util
from pyspark.sql.functions import col, struct, lit, concat, array
from . import paymentPending as PP
from . import AwaitingEvidenceRespondant_a as AERa

def generalDefault(silver_m1): 
    paymentPending_generalDefault_content, paymentPending_generalDefault_audit = AERa.generalDefault(silver_m1) 
    
    # Update column changeDirectionDueDateActionAvailable and add two new colums per mapping document 
    df_awaitingEvidenceRespondent_b = (
        paymentPending_generalDefault_content
        .withColumn("uploadHomeOfficeBundleActionAvailable",lit("No"))
    )
            
    return paymentPending_generalDefault_content, paymentPending_generalDefault_audit

def documents(silver_m1): 
    documents_content, documents_audit = PP.documents(silver_m1)

    awaitingEvidenceRespondent_b_documents_content = (
        documents_content
        .withColumn("respondentDocuments",array())
    )
    return awaitingEvidenceRespondent_b_documents_content, documents_audit

if __name__ == "__main__":
    pass

