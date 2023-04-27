import numpy as np
import pandas as pd

"""
This file contains the list of values to map values to each other
during when using the Cleanser class. 

It defines one large, nested dictionary called value_mapper. 

This dictionary contains as keys the columns of the final SDMX data structure. 
The values of those keys are dictionaries. 

These dictionaries contain as key the target value (i.e. the value 
that a the column is supposed to be able to take). The value of 
the keys is a list containing all values in the raw dataframes
which are supposed to be mapped into that value. 

The schema of the dictionar is thus: 

value_mapper = {
    <column name in final sdmx df> : {
        <target value> : <list of values to be converted into the target value> 
    }
}
"""

value_mapper = {
    "DIM_SEX": {
        "BOTH_SEXES": [
            "_T",
            "BOTHSEX",
            "BTSX",
            "SEX_T",
            "Total",
            "TOTAL: Total",
            "_T: Total",
        ],
        "MALE": ["M", "MALE", "MLE", "SEX_M", "Male", "M: Male"],
        "FEMALE": ["F", "FEMALE", "FMLE", "SEX_F", "Female", "F: Female"],
    },
    "DIM_EDU_LEVEL": {
        "LOWER SECONDARY EDUCATION": ["L2"],
        "UPPER SECONDARY EDUCATION": ["L3"],
        "PRE-PRIMARY EDUCATION": ["L02"],
        "PRIMARY EDUCATION": ["L1"],
        "_T": [""],
    },
    "DIM_AGE_GROUP": {
        "_T": ["_T", "Total"],
        "10-19 YEARS": ["YEARS10-19"],
        "5-19 YEARS": ["YEARS05-19"],
        "5-09 YEARS": ["YEARS05-09"],
        "5-17 YEARS": ["5-17"],
        "5-14 YEARS": ["5-14"],
        "7-17 YEARS": ["7-17"],
        "6-17 YEARS": ["6-17"],
        "10-17 YEARS": ["10-17"],
        "18-29 YEARS": ["18-29"],
        "18-74 YEARS": ["18-74"],
        "ALL AGES": ["ALLAGE"],
        "15-24 YEARS": ["15-24"],
        "15+ YEARS": ["15+"],
        "25+ YEARS": ["25+"],
        "13-15 YEARS": ["13 to 15 years old"],
        "13-17 YEARS": ["13 to 17 years old"],
        "15 YEARS": ["15 years old"],
        "0-5 YEARS": ["UFIVE: Under five", "Y0T4: Under 5 years old"],
        "POST-NEONATAL": ["POST: Post-neonatal"],
        "NEO-NATAL": ["NEO: Neo-natal"],
        "<1 YEAR": ["<1Y"],
        "<5 YEARS": ["<5Y"],
        "<6 YEARS": ["<6Y"],
        "<8 YEARS": ["<8Y"],
        "5-59 MONTHS": ["M6T59"],
        "SCHOOL_AGE_POPULATION": ["SCH_AGE_GROUP"],
    },
    "DIM_MANAGEMENT_LEVEL": {
        "OCU_MGMT_SENIOR": ["OCU_MGMT_SENIOR"],
        "OCU_MGMT_TOTAL": ["OCU_MGMT_TOTAL"],
        "_T": [""],
    },
    "DIM_AREA_TYPE": {
        "TOTAL": ["TOTL", "_T: Total"],
        "RURAL": ["RUR", "R: Rural"],
        "URBAN": ["URB", "U: Urban"],
        "_T": [""],
    },
    "DIM_QUANTILE": {
        "First quantile (Q1)": ["Q1"],
        "Lowest (Q1)": ["Q1: Lowest"],
        "Second (Q2)": ["Q2: Second"],
        "Middle (Q3)": ["Q3: Middle"],
        "Fourth (Q4)": ["Q4: Fourth"],
        "Highest (Q5)": ["Q5: Highest"],
        "_T": ["_T", "_T: Total"],
        "Bottom 20%": ["B20: Bottom 20%"],
        "Bottom 40%": ["B40: Bottom 40%"],
        "Bottom 60%": ["B60: Bottom 60%"],
        "Bottom 80%": ["B80: Bottom 80%"],
        "Richest 20%": ["R20: Richest 20%"],
        "Richest 40%": ["R40: Richest 40%"],
        "Richest 60%": ["R60: Richest 60%"],
        "Richest 80%": ["R80: Richest 80%"],
    },
    "DIM_SDG_INDICATOR": {
        "1.1.1": ["['1.1.1']"],
        "1.a.2": ["['1.a.2']"],
        "1.3.1": ["['1.3.1']"],
        "1.5.1": ["['1.5.1']"],
        "1.5.3": ["['1.5.3']"],
        "1.5.4": ["['1.5.4']"],
        "2.1.2": ["['2.1.2']"],
        "3.9.2": ["['3.9.2']"],
        "8.3.1": ["['8.3.1']"],
        "8.5.1": ["['8.5.1']"],
        "8.7.1": ["['8.7.1']"],
        "11.5.2": ["['11.5.2']"],
        "11.5.3": ["['11.5.3']"],
        "11.b.1": ["['11.b.1']"],
        "11.b.2": ["['11.b.2']"],
        "11.5.1": ["['11.5.1']"],
        "12.4.1": ["['12.4.1']"],
        "12.4.2": ["['12.4.2']"],
        "12.5.1": ["['12.5.1']"],
        "12.1.1": ["['12.1.1']"],
        "13.1.1": ["['13.1.1']"],
        "13.1.2": ["['13.1.2']"],
        "13.1.3": ["['13.1.3']"],
        "15.3.1": ["['15.3.1']"],
        "16.2.2": ["['16.2.2']"],
        "16.2.3": ["['16.2.3']"],
        "16.9.1": ["['16.9.1']"],
    },
    # A lot of the values come from source S-203, follow this link to obtain
    # raw data encodings: https://unstats.un.org/SDGAPI/v1/sdg/Series/SL_EMP_AEARN/Dimensions
    "DIM_OCU_TYPE": {
        "All occupations (isco-08)": ["isco08", "OCU_ISCO08_TOTAL"],
        "Armed forces occupations (isco-08)": ["isco08-0", "OCU_ISCO08_0"],
        "Managers (isco-08)": ["isco08-1", "OCU_ISCO08_1"],
        "Professionals (isco-08)": ["isco08-2", "OCU_ISCO08_2"],
        "Technicians and associate professionals (isco-08)": [
            "isco08-3",
            "OCU_ISCO08_3",
        ],
        "Clerical support workers (isco-08)": ["isco08-4", "OCU_ISCO08_4"],
        "Service and sales workers (isco-08)": ["isco08-5", "OCU_ISCO08_5"],
        "Skilled agricultural, forestry and fishery workers (isco-08)": [
            "isco08-6",
            "OCU_ISCO08_6",
        ],
        "Craft and related trades workers (isco-08)": ["isco08-7", "OCU_ISCO08_7"],
        "Plant and machine operators, and assemblers (isco-08)": [
            "isco08-8",
            "OCU_ISCO08_8",
        ],
        "Elementary occupations (isco-08)": ["isco08-9", "OCU_ISCO08_9"],
        "Not elsewhere classified (isco-08)": ["isco08-X", "OCU_ISCO08_X"],
        "All occupations (isco-88)": ["isco88", "OCU_ISCO88_TOTAL"],
        "Armed forces (isco-88)": ["isco88-0"],
        "Legislators, senior officials and managers (isco-88)": ["isco88-1"],
        "Professionals (isco-88)": ["isco88-2"],
        "Technicians and associate professionals (isco-88)": ["isco88-3"],
        "Clerks (isco-88)": ["isco88-4"],
        "Service workers and shop and market sales workers (isco-88)": ["isco88-5"],
        "Skilled agricultural and fishery workers (isco-88)": ["isco88-6"],
        "Craft and related trades workers (isco-88)": ["isco88-7"],
        "Plant and machine operators and assemblers (isco-88)": ["isco88-8"],
        "Elementary occupations (isco-88)": ["isco88-9"],
        "Not elsewhere classified (isco-88)": ["isco88-X"],
    },
    "DIM_REP_TYPE": {"Global": ["G"], "National": ["N"]},
    "DIM_SECTOR": {
        "NO BREAKDOWN": ["TOTAL"],
        "Agriculture, forestry and fishing": ["ISIC4_A"],
        "Non-agriculture": ["NONAGR"],
        "_T": [""],
        "Oil and gas": ["Oil and gas"],
        "Mining": ["Mining"],
    },
    "ATTR_SOURCE_UNIT_MEASURE": {
        "PERCENT": ["PT", "PERCENT"],
        "HR": ["HR"],
        "CUR_LCU": ["CUR_LCU"],
        "NUMBER": ["NUMBER"],
        "PER_100000_POP": ["PER_100000_POP"],
        "": [""],
        "KG": ["KG"],
    },
    "DIM_POLICY_TYPE": {
        "_T": ["_T"],
        "Macro policies (e.g. national strategies/action plans, new institutions/entities)": [
            "POLICY_MACRO"
        ],
        "Economic and fiscal instruments (taxes and tax incentives, grants, preferential loans, etc.)": [
            "POLICY_ECONFIS"
        ],
        "Regulatory and legal instruments (e.g. laws, standards, enforcement measures)": [
            "POLICY_REGLEG"
        ],
        "Voluntary and self-regulation schemes (e.g. sectoral partnerships, codes of conduct, CSR initiatives)": [
            "POLICY_VOLSRG"
        ],
    },
}
