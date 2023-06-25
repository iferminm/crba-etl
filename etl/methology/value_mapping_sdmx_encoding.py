import numpy as np
import pandas as pd
"""
This is the dictionary to convert values from dimensions
into code lists to make the dataset sdmx-conform and 
ready for upload.
"""

value_mapper_sdmx_encoding = {
    "INDICATOR_INDEX": {
        "WP": ["Workplace"],
        "MP": ["Marketplace"],
        "CE": ["Community and Environment"],
    },
    "INDICATOR_ISSUE": {
        "ONLINE_ABUSE_EXPL": ["Online Abuse and exploitation"],
        "CHLD_LABOUR": ["Child labour"],
        "MARKETING_ADV": ["Marketing and Advertising"],
        "PROD_SAFETY": ["Product Safety"],
        "RESOURCE_ENV_DAMAGE": ["Resource use and damage to the environment"],
        "LAND_RIGHTS": ["Land rights"],
        "SECURITY_ARRANGMENTS": ["Security arrangements"],
        "NAT_DISASTERS": ["Natural disasters"],
        "CH_RIGHTS_FULLF": ["Fulfillment of children’s rights"],
        "DECENT_WORK_COND": ["Decent working conditions"],
        "MAT_PAT_PROT": ["Maternity and paternity protection"],
    },
    "INDICATOR_CATEGORY": {
        "OUTCOME": ["Outcome"],
        "ENFORCEMENT": ["Enforcement"],
        "LEG_FRAMEWORK_NAT": ["Legal framework national"],
        "LEG_FRAMEWORK_INTERNAT": ["Legal framework international"],
    },
    "DIM_AGE_GROUP": {
        "Y18T28": ["18-29 YEARS"],
        "Y18T73": ["18-74 YEARS"],
        "Y13T14": ["13-15 YEARS"],
        "Y15": ["15 YEARS"],
        "Y13T16": ["13-17 YEARS"],
        "Y5T16": ["5-17 YEARS"],
        "Y5T13": ["5-14 YEARS"],
        "Y7T16": ["7-17 YEARS"],
        "Y6T16": ["6-17 YEARS"],
        "Y10T16": ["10-17 YEARS"],
        "_T": ["ALL AGES"],
        "Y0T4": ["<5 YEARS", "<5 YEARS", "0-5 YEARS"],
        "Y0": ["<1 YEAR"],
        "Y0T7": ["<8 YEARS"],
        "Y0T5": ["<6 YEARS"],
        "M5T58": ["5-59 MONTHS"],
        "Y_GE15": ["15+ YEARS"],
        "Y15T23": ["15-24 YEARS"],
        "Y_GE25": ["25+ YEARS"],
        "Y5T18": ["5-19 YEARS"],
        "Y5T8": ["5-09 YEARS"],
        "Y10T18": ["10-19 YEARS"],
        "POST_NEONATAL": ["POST-NEONATAL"],
        "NEO_NATAL": ["NEO-NATAL"],
        "SCH_AGE_GROUP": ["SCHOOL_AGE_POPULATION"],
    },
    "DIM_SEX": {
        "_T": ["BOTH_SEXES"],
        "M": ["MALE"],
        "F": ["FEMALE"],
    },
    "DIM_SECTOR": {
        "_T": ["NO BREAKDOWN", "_T"],
        "MINING": ["Mining"],
        "OIL_GAS": ["Oil and gas"],
        "NON_AGRIC": ["Non-agriculture"],
        "AGRIC_FOR_FISH": ["Agriculture, forestry and fishing"],
    },
    "DIM_AREA_TYPE": {
        "_T": ["_T", "TOTAL"],
        "R": ["RURAL"],
        "U": ["URBAN"],
    },
    "DIM_EDU_LEVEL": {
        "_T": ["_T"],
        "ISCED11_1": ["PRIMARY EDUCATION"],
        "ISCED11_02": ["PRE-PRIMARY EDUCATION"],
        "ISCED11_2": ["LOWER SECONDARY EDUCATION"],
        "ISCED11_3": ["UPPER SECONDARY EDUCATION"],
    },
    "DIM_MATERNAL_EDU_LVL": {
        "_T": ["_T", "_T: Total"],
        "AGG_0_1": ["AGG_0_1: None and Primary"],
        "AGG_2_3": [
            "AGG_2_3: Secondary education (lower and upper secondary education)"
        ],
        "AGG_3S_H": ["AGG_3S_H: Secondary and Higher"],
        "AGG_5T8": ["AGG_5T8: Tertiary education"],
        "ISCED11A_01": [
            "ISCED11A_01: Never attended an education programme / No schooling"
        ],
        "ISCED11_1": ["ISCED11_1: Primary education"],
    },
    "DIM_QUANTILE": {
        "_T": ["_T"],
        "B20": ["Bottom 20%"],
        "B40": ["Bottom 40%"],
        "B60": ["Bottom 60%"],
        "B80": ["Bottom 80%"],
        "Q1": ["Lowest (Q1)", "Lowest (Q1) ", "First quantile (Q1)"],
        "Q2": ["Second (Q2)"],
        "Q3": ["Middle (Q3)"],
        "Q4": ["Fourth (Q4)"],
        "Q5": ["Highest (Q5)"],
        "R20": ["Richest 20%"],
        "R40": ["Richest 40%"],
        "R60": ["Richest 60%"],
        "R80": ["Richest 80%"],
    },
    "DIM_OCU_TYPE": {
        "_T": ["All occupations (isco-08)", "All occupations (isco-88)", "_T"],
        "MANAGERS": ["Managers (isco-08)"],
        "PROFESSIONALS": ["Professionals (isco-08)"],
        "TECHNICIANS": ["Technicians and associate professionals (isco-08)"],
        "CLERICAL_SUPPORT": ["Clerical support workers (isco-08)"],
        "SERVICE_SALES": ["Service and sales workers (isco-08)"],
        "AGRIC_FOR_FISH": [
            "Skilled agricultural, forestry and fishery workers (isco-08)"
        ],
        "CRAFT_AND_TRADES": ["Craft and related trades workers (isco-08)"],
        "PLANT_MACHINE_ASSEMBLERS": [
            "Plant and machine operators, and assemblers (isco-08)"
        ],
        "ELEMENTARY": ["Elementary occupations (isco-08)"],
        "ARMED_FORCES": ["Armed forces occupations (isco-08)"],
        "NOT_CLASSIFIED": ["Not elsewhere classified (isco-08)"],
    },
    "DIM_CAUSE_TYPE": {
        "_T": ["_T"],
        "PRETERM": ["CH10: Preterm"],
        "INTRAPARTUM_EVENT": ["CH11: Intrapartum-related events"],
        "SEPSIS": ["CH12: Sepsis"],
        "OTHER": ["CH13: Other"],
        "CONGENITAL": ["CH15: Congenital"],
        "NCDS": ["CH16: NCDs"],
        "INJURIES": ["CH17: Injuries"],
        "AIDS": ["CH2: AIDS"],
        "DIARRHOEA": ["CH3: Diarrhoea"],
        "PERTUSSIS": ["CH4: Pertussis"],
        "TETANUS": ["CH5: Tetanus"],
        "MEASLES": ["CH6: Measles"],
        "MENINGITIS": ["CH7: Meningitis"],
        "MALARIA": ["CH8: Malaria"],
        "PREUMONIA": ["CH9: Pneumonia"],
    },
    "DIM_MANAGEMENT_LEVEL": {
        "_T": ["_T", "OCU_MGMT_TOTAL"],
        "OCU_MGMT_SENIOR": ["OCU_MGMT_SENIOR"],
    },
}