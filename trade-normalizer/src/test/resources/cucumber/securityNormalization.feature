Feature: Security Normalization

  Scenario: 1 - normalize trade with security on isin

    Given receive the following security records:
      | securityId | isin    |
      | secId      | secIsin |

    Given receive the following trade records:
      | isin    | qty |
      | secIsin | 10  |

    Then output the following trade records:
      | securityId | qty |
      | secId      | 10  |


  Scenario: 2 - trade security does not exist

    Given receive the following trade records:
      | isin    | qty |
      | secIsin | 10  |

    Then output the following trade records:
      | securityId | qty |

    Given time passes