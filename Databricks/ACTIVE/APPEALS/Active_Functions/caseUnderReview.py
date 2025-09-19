import importlib.util
from pyspark.sql.functions import col, struct, lit, concat, array
from . import paymentPending as PP
from . import AwaitingEvidenceRespondant_a as AERa
from . import AwaitingEvidenceRespondant_b as AERb


def generalDefault(silver_m1):
    df = AERb.generalDefaults(silver_m1)

    df.select(
        *,
        lit("Yes").alias("caseArgumentAvailable")
    )

    return df

def hearingDetails(silver_m3,silver_m6):

    
