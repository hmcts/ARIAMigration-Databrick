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

    # Add all checks in state
    for state_to_process in state_flow:
        checks = state_dq_rules_map(state_to_process).add_checks(checks)

    return checks


def state_dq_rules_map(state: str) -> DQRulesBase:
    match state:
        case "paymentPending": return paymentpending_dq_rules.paymentPendingDQRules(),
        case "appealSubmitted": return appealSubmitted_dq_rules.appealSubmittedDQRules(),
        case "awaitingRespondentEvidence(a)": return awaitingEvidenceRespondentA_dq_rules.awaitingEvidenceRespondentADQRules(),
        case "awaitingRespondentEvidence(b)": return awaitingEvidenceRespondentB_dq_rules.awaitingEvidenceRespondentBDQRules(),
        case "caseUnderReview": return caseUnderReview_dq_rules.caseUnderReviewDQRules(),
        case "reasonForAppealSubmitted": return reasonsForAppealSubmitted_dq_rules.reasonsForAppealSubmittedDQRules(),
        case "listing": return listing_dq_rules.listingDQRules(),
        case "prepareForHearing": return prepareforhearing_dq_rules.prepareForHearingDQRules(),
        case "decision": return decision_dq_rules.decisionDQRules(),
        case "decided(a)": return decided_a_dq_rules.decidedADQRules(),
        case "ftpaSubmitted(b)": return ftpa_submitted_b_dq_rules.ftpaSubmittedBDQRules(),
        case "ftpaSubmitted(a)": return ftpa_submitted_a_dq_rules.ftpaSubmittedADQRules(),
        case "ftpaDecided": return DQRulesBase(),
        case "ended": return DQRulesBase(),
        case "remitted": return DQRulesBase(),
        case _: return DQRulesBase()


def previous_state_map(state: str):
    match state:
        case "appealSubmitted":               return "paymentPending", 
        case "awaitingRespondentEvidence(a)": return "appealSubmitted",
        case "awaitingRespondentEvidence(b)": return "awaitingRespondentEvidence(a)",
        case "caseUnderReview":               return "awaitingRespondentEvidence(b)",
        case "reasonForAppealSubmitted":      return "awaitingRespondentEvidence(b)",
        case "listing":                       return "awaitingRespondentEvidence(b)",
        case "prepareForHearing":             return "listing",
        case "decision":                      return "prepareHearing",
        case "decided(a)":                    return "decision",
        case "ftpaSubmitted(b)":              return "decided(a)",
        case "ftpaSubmitted(a)":              return "ftpaSubmitted(b)",
        case "ftpaDecided":                   return "ftpaSubmitted(a)",
        case "ended":                         return "ftpaDecided",
        case "remitted":                      return "ended"
        case _:                               return None


# Implement DQ checks based on previous state
def build_state_flow(state: str, flow: list):
    # Define the previous state for the given state
    previous_state = previous_state_map(state)
    if previous_state is None:
        return flow
    return build_state_flow(previous_state, [previous_state] + flow)


if __name__ == "__main__":
    pass
