# agents/critic_agent.py
# Quality gate — scores every output 0-10 before delivery.
# Dimensions: Specificity, Actionability, Accuracy, Completeness, Tone.
# Score >= 8: pass | 6-7: enhance | < 6: rewrite
# Model: Claude Haiku 4.5
# Built in Phase 7.
