from Databricks.ACTIVE.APPEALS.shared_functions.dq_rules import (
    paymentpending_dq_rules, appealSubmitted_dq_rules, awaitingEvidenceRespondentA_dq_rules, awaitingEvidenceRespondentB_dq_rules,
    caseUnderReview_dq_rules, reasonsForAppealSubmitted_dq_rules, listing_dq_rules, prepareforhearing_dq_rules, decision_dq_rules,
    decided_a_dq_rules, ftpa_submitted_b_dq_rules, ftpa_submitted_a_dq_rules
)
from Databricks.ACTIVE.APPEALS.shared_functions.dq_rules.dq_rules import DQRulesBase


def build_rule_expression(rules: dict) -> str:
    """
    Joins multiple rule expressions into one combined SQL expression.
    """
    return "({0})".format(" AND ".join(rules.values()))


def base_DQRules(state: str = "paymentPending"):
    """
    Return a dictionary of the DQ rules to be used in the expectations
    """

    checks = {}

    state_flow = build_state_flow(state, [state])

    # Add all checks in state, errors if state not in mapping.
    for state_to_process in state_flow:
        checks = state_dq_rules_map(state_to_process).add_checks(checks)

    return checks


def state_dq_rules_map(state: str) -> DQRulesBase:
    dq_rules = {
        "paymentPending": paymentpending_dq_rules.paymentPendingDQRules(),
        "appealSubmitted": appealSubmitted_dq_rules.appealSubmittedDQRules(),
        "awaitingRespondentEvidence(a)": awaitingEvidenceRespondentA_dq_rules.awaitingEvidenceRespondentADQRules(),
        "awaitingRespondentEvidence(b)": awaitingEvidenceRespondentB_dq_rules.awaitingEvidenceRespondentBDQRules(),
        "caseUnderReview": caseUnderReview_dq_rules.caseUnderReviewDQRules(),
        "reasonForAppealSubmitted": reasonsForAppealSubmitted_dq_rules.reasonsForAppealSubmittedDQRules(),
        "listing": listing_dq_rules.listingDQRules(),
        "prepareForHearing": prepareforhearing_dq_rules.prepareForHearingDQRules(),
        "decision": decision_dq_rules.decisionDQRules(),
        "decided(a)": decided_a_dq_rules.decidedADQRules(),
        "ftpaSubmitted(b)": ftpa_submitted_b_dq_rules.ftpaSubmittedBDQRules(),
        "ftpaSubmitted(a)": ftpa_submitted_a_dq_rules.ftpaSubmittedADQRules(),
        "ftpaDecided": None,
        "ended": None,
        "remitted": None
    }

    return dq_rules.get(state, None)


def previous_state_map(state: str):
    previous_state = {
        "appealSubmitted":               "paymentPending",
        "awaitingRespondentEvidence(a)": "appealSubmitted",
        "awaitingRespondentEvidence(b)": "awaitingRespondentEvidence(a)",
        "caseUnderReview":               "awaitingRespondentEvidence(b)",
        "reasonForAppealSubmitted":      "awaitingRespondentEvidence(b)",
        "listing":                       "awaitingRespondentEvidence(b)",
        "prepareForHearing":             "listing",
        "decision":                      "prepareHearing",
        "decided(a)":                    "decision",
        "ftpaSubmitted(b)":              "decided(a)",
        "ftpaSubmitted(a)":              "ftpaSubmitted(b)",
        "ftpaDecided":                   "ftpaSubmitted(a)",
        "ended":                         "ftpaDecided",
        "remitted":                      "ended"
    }

    return previous_state.get(state, None)


# Implement DQ checks based on previous state
def build_state_flow(state: str, flow: list):
    # Define the previous state for the given state
    previous_state = previous_state_map(state)
    if previous_state is None:
        return flow
    return build_state_flow(previous_state, [previous_state] + flow)


if __name__ == "__main__":
    pass
