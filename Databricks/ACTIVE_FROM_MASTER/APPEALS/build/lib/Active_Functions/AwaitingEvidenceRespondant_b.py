import importlib.util
from pyspark.sql.functions import col, struct, lit, concat, array
from . import paymentPending as PP
from . import AwaitingEvidenceRespondant_a as AERa

def generalDefault(silver_m1): 
    df_generalDefault = AERa.generalDefault(silver_m1)
    
    df_generalDefault = (
        df_generalDefault
        .select("*",
                lit("No").alias("uploadHomeOfficeBundleActionAvailable"))
    )
            
    return df_generalDefault

def documents(silver_m1): 
    documents_df, documents_audit = PP.documents(silver_m1)

    documents_df = (
        documents_df
        .select("*",
                lit([]).cast("array<string>").alias("respondentDocuments"))
    )
    return documents_df, documents_audit

if __name__ == "__main__":
    pass

