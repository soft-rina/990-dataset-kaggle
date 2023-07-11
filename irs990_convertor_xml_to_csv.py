import pandas as pd
import xmltodict
import xml
import os 
import time
from itertools import chain

# Dictionary for filtering and mapping columns
COLUMNS_NAME_MAP = {
    'Return.ReturnHeader.ReturnTypeCd':'return_type', 
    'Return.ReturnHeader.Filer.EIN':'ein', 
    'Return.ReturnHeader.Filer.BusinessNameControlTxt':'business_name_control', 
    'Return.ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt':'business_name_ln1',
    'Return.ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt':'business_name_ln2', 
    'Return.ReturnHeader.Filer.USAddress.ZIPCd':'zip_code', 
    'Return.ReturnHeader.Filer.USAddress.AddressLine1Txt':'address', 
    'Return.ReturnHeader.Filer.USAddress.CityNm':'city', 
    'Return.ReturnHeader.Filer.USAddress.StateAbbreviationCd':'state', 
    'Return.ReturnData.IRS990.PrincipalOfficerNm':'principal_officer', 
    'Return.ReturnData.IRS990.GrossReceiptsAmt':'gross_receipts', 
    'Return.ReturnData.IRS990.Organization501c3Ind':'501c3_org', 
    'Return.ReturnData.IRS990.WebsiteAddressTxt':'website', 
    'Return.ReturnData.IRS990EZ.WebsiteAddressTxt':'website_ez', 
    'Return.ReturnData.IRS990.TypeOfOrganizationCorpInd':'org_type_corporation', 
    'Return.ReturnData.IRS990EZ.TypeOfOrganizationCorpInd':'org_type_corporation_ez', 
    'Return.ReturnData.IRS990.TypeOfOrganizationTrustInd':'org_type_trust', 
    'Return.ReturnData.IRS990EZ.TypeOfOrganizationTrustInd':'org_type_trust_ez', 
    'Return.ReturnData.IRS990.TypeOfOrganizationAssocInd':'org_type_association', 
    'Return.ReturnData.IRS990EZ.TypeOfOrganizationAssocInd':'org_type_association_ez', 
    'Return.ReturnData.IRS990.TypeOfOrganizationOtherInd':'org_type_other', 
    'Return.ReturnData.IRS990EZ.TypeOfOrganizationOtherInd':'org_type_other_ez', 
    'Return.ReturnData.IRS990EZ.TypeOfOrganizationOtherDesc':'org_type_other_description', 
    'Return.ReturnData.IRS990.FormationYr':'formation_year', 
    'Return.ReturnData.IRS990.LegalDomicileStateCd':'legal_domicile_state', 
    'Return.ReturnData.IRS990.ActivityOrMissionDesc':'activity_or_mission_description', 
    'Return.ReturnData.IRS990.VotingMembersGoverningBodyCnt':'num_governing_body_voting_members', 
    'Return.ReturnData.IRS990.VotingMembersIndependentCnt':'num_independent_voting_members', 
    'Return.ReturnData.IRS990.TotalEmployeeCnt':'total_num_employees', 
    'Return.ReturnData.IRS990.TotalVolunteersCnt':'total_num_volunteers', 
    'Return.ReturnData.IRS990.TotalGrossUBIAmt':'total_unrelated_business_income', 
    'Return.ReturnData.IRS990.NetUnrelatedBusTxblIncmAmt':'net_unrelated_business_taxable_income', 
    'Return.ReturnData.IRS990.PYContributionsGrantsAmt':'contribution_grants_py',
    'Return.ReturnData.IRS990.CYContributionsGrantsAmt':'contribution_grants_cy', 
    'Return.ReturnData.IRS990.PYProgramServiceRevenueAmt':'program_service_revenue_py',
    'Return.ReturnData.IRS990.CYProgramServiceRevenueAmt':'program_service_revenue_cy', 
    'Return.ReturnData.IRS990.PYInvestmentIncomeAmt':'investment_income_py', 
    'Return.ReturnData.IRS990.CYInvestmentIncomeAmt':'investment_income_cy', 
    'Return.ReturnData.IRS990.PYOtherRevenueAmt':'other_revenue_py', 
    'Return.ReturnData.IRS990.CYOtherRevenueAmt':'other_revenue_cy', 
    'Return.ReturnData.IRS990.PYTotalRevenueAmt':'total_revenue_py', 
    'Return.ReturnData.IRS990.CYTotalRevenueAmt':'total_revenue_cy', 
    'Return.ReturnData.IRS990.PYGrantsAndSimilarPaidAmt':'grants_and_similar_amounts_paid_py', 
    'Return.ReturnData.IRS990.CYGrantsAndSimilarPaidAmt':'grants_and_similar_amounts_paid_cy', 
    'Return.ReturnData.IRS990.PYBenefitsPaidToMembersAmt':'benefits_paid_to_members_py', 
    'Return.ReturnData.IRS990.CYBenefitsPaidToMembersAmt':'benefits_paid_to_members_cy',
    'Return.ReturnData.IRS990.PYSalariesCompEmpBnftPaidAmt':'salaries_compensations_emp_benefits_paid_py',
    'Return.ReturnData.IRS990.CYSalariesCompEmpBnftPaidAmt':'salaries_compensations_emp_benefits_paid_cy',
    'Return.ReturnData.IRS990.PYTotalProfFndrsngExpnsAmt':'total_professional_fundraising_expense_py', 
    'Return.ReturnData.IRS990.CYTotalProfFndrsngExpnsAmt':'total_professional_fundraising_expense_cy',
    'Return.ReturnData.IRS990.CYTotalFundraisingExpenseAmt':'total_fundrasing_expense_cy',
    'Return.ReturnData.IRS990.PYOtherExpensesAmt':'other_expenses_py',
    'Return.ReturnData.IRS990.CYOtherExpensesAmt':'other_expenses_cy',
    'Return.ReturnData.IRS990.PYTotalExpensesAmt':'total_expenses_py',
    'Return.ReturnData.IRS990.CYTotalExpensesAmt':'total_expenses_cy',
    'Return.ReturnData.IRS990.PYRevenuesLessExpensesAmt':'revenue_less_expenses_py',
    'Return.ReturnData.IRS990.CYRevenuesLessExpensesAmt':'revenue_less_expenses_cy',
    'Return.ReturnData.IRS990.TotalAssetsBOYAmt':'total_assests_boy',
    'Return.ReturnData.IRS990.TotalAssetsEOYAmt':'total_assests_eoy',
    'Return.ReturnData.IRS990.TotalLiabilitiesBOYAmt':'total_liabilities_boy',
    'Return.ReturnData.IRS990.TotalLiabilitiesEOYAmt':'total_liabilities_eoy',
    'Return.ReturnData.IRS990.NetAssetsOrFundBalancesBOYAmt':'net_assets_fund_balances_boy',
    'Return.ReturnData.IRS990.NetAssetsOrFundBalancesEOYAmt':'net_assets_fund_balances_eoy',
    'Return.ReturnData.IRS990.MissionDesc':'mission_description', 
    'Return.ReturnData.IRS990.SignificantNewProgramSrvcInd':'significant_new_program_services',
    'Return.ReturnData.IRS990.SignificantChangeInd':'significant_change_program_services',
    'Return.ReturnData.IRS990.ExpenseAmt':'program_expense_amount', 
    'Return.ReturnData.IRS990.GrantAmt':'program_grant_amount', 
    'Return.ReturnData.IRS990.RevenueAmt':'program_revenue_amount',
    'Return.ReturnData.IRS990.Desc':'other_program_services_description',
    'Return.ReturnData.IRS990.TotalProgramServiceExpensesAmt':'total_program_service_expenses',
    'Return.ReturnData.IRS990.DescribedInSection501c3Ind':'described_in_501c3', 
    'Return.ReturnData.IRS990.PoliticalCampaignActyInd':'political_campaign_activity',
    'Return.ReturnData.IRS990.LobbyingActivitiesInd':'lobbying_activities',
    'Return.ReturnData.IRS990.ConservationEasementsInd':'conservation_easement',
    'Return.ReturnData.IRS990.CollectionsOfArtInd':'collection_of_art',
    'Return.ReturnData.IRS990.CreditCounselingInd':'credit_counseling',
    'Return.ReturnData.IRS990.SchoolOperatingInd':'school_operating',
    'Return.ReturnData.IRS990.ForeignOfficeInd':'foreign_office', 
    'Return.ReturnData.IRS990.ForeignActivitiesInd':'foreign_activities',
    'Return.ReturnData.IRS990.MoreThan5000KToOrgInd':'grants_assistance_foreign_orgs_over500k',
    'Return.ReturnData.IRS990.MoreThan5000KToIndividualsInd':'grants_assistance_foreign_individuals_over500k',
    'Return.ReturnData.IRS990.ProfessionalFundraisingInd':'professional_fundraising_over15k',
    'Return.ReturnData.IRS990.FundraisingActivitiesInd':'fundraising_activities_over15k',
    'Return.ReturnData.IRS990.GamingActivitiesInd':'gaming_activities_over15k',
    'Return.ReturnData.IRS990.OperateHospitalInd':'operating_hospital',
    'Return.ReturnData.IRS990.GrantsToOrganizationsInd':'grants_domestic_orgs_over5k',
    'Return.ReturnData.IRS990.GrantsToIndividualsInd':'grants_domestic_individuals_over5k',
    'Return.ReturnData.IRS990.LoanOutstandingInd':'outstanding_loan',
    'Return.ReturnData.IRS990.BusinessRlnWithOrgMemInd':'business_with_org_members',
    'Return.ReturnData.IRS990.BusinessRlnWithFamMemInd':'business_with_org_members_family',
    'Return.ReturnData.IRS990.BusinessRlnWithOfficerEntInd':'business_with_org_officers',
    'Return.ReturnData.IRS990.ForeignFinancialAccountInd':'foreign_financial_account',
    'Return.ReturnData.IRS990.ProhibitedTaxShelterTransInd':'prohibited_tax_shelter_transaction',
    'Return.ReturnData.IRS990.NondeductibleContributionsInd':'nondeductible_contributions_over100K',  
    'Return.ReturnData.IRS990.QuidProQuoContributionsInd':'quid_pro_quo_contributions',
    'Return.ReturnData.IRS990.FamilyOrBusinessRlnInd':'family_business_relationship',
    'Return.ReturnData.IRS990.MembersOrStockholdersInd':'members_stockholders',
    'Return.ReturnData.IRS990.LocalChaptersInd':'local_chapters',
    'Return.ReturnData.IRS990.ConflictOfInterestPolicyInd':'conflict_of_interest_policy', 
    'Return.ReturnData.IRS990.AnnualDisclosureCoveredPrsnInd':'annual_conflicts_disclousre', 
    'Return.ReturnData.IRS990.RegularMonitoringEnfrcInd':'enforced_compliancy', 
    'Return.ReturnData.IRS990.WhistleblowerPolicyInd':'whistleblower_policy', 
    'Return.ReturnData.IRS990.DocumentRetentionPolicyInd':'document_retention_policy',
    'Return.ReturnData.IRS990.CompensationProcessCEOInd':'compensation_process_top_management',
    'Return.ReturnData.IRS990.CompensationProcessOtherInd':'compensation_process_others',
    'Return.ReturnData.IRS990.InvestmentInJointVentureInd':'investment_in_joint_venture',
    'Return.ReturnData.IRS990.TotalReportableCompFromOrgAmt':'total_reportable_compensation_from_org',
    'Return.ReturnData.IRS990.TotReportableCompRltdOrgAmt':'total_reportable_compensation_from_related_org',
    'Return.ReturnData.IRS990.TotalOtherCompensationAmt':'total_other_compensations', 
    'Return.ReturnData.IRS990.IndivRcvdGreaterThan100KCnt':'num_individuals_recieved_over100k',
    'Return.ReturnData.IRS990.TotalCompGreaterThan150KInd':'compensation_over_150k',
    'Return.ReturnData.IRS990.CompensationFromOtherSrcsInd':'compensation_from_other_sources',
    'Return.ReturnData.IRS990.CntrctRcvdGreaterThan100KCnt':'total_num_contractors_comp_over100k',
    'Return.ReturnData.IRS990.TotalRevenueGrp.TotalRevenueColumnAmt':'total_revenue',
    'Return.ReturnData.IRS990.TotalRevenueGrp.RelatedOrExemptFuncIncomeAmt':'total_related_exempt_income',
    'Return.ReturnData.IRS990.TotalRevenueGrp.UnrelatedBusinessRevenueAmt':'total_unrelated_businenss_revenue',
    'Return.ReturnData.IRS990.TotalRevenueGrp.ExclusionAmt':'total_exclusion_amount',
    'Return.ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt':'grants_to_domestic_groups_total',
    'Return.ReturnData.IRS990.GrantsToDomesticOrgsGrp.ProgramServicesAmt':'grants_to_domestic_groups_program_services_total',
    'Return.ReturnData.IRS990.TotalFunctionalExpensesGrp.ManagementAndGeneralAmt':'management_general_expenses_total',
    'Return.ReturnData.IRS990.TotalFunctionalExpensesGrp.TotalAmt':'functional_expenses_total',
    'Return.ReturnData.IRS990.TotalFunctionalExpensesGrp.FundraisingAmt':'fundraising_total',
    'Return.ReturnData.IRS990.TotalFunctionalExpensesGrp.ProgramServicesAmt':'program_services_total',
    'Return.ReturnData.IRS990.TotalAssetsGrp.EOYAmt':'total_assets_eoy',
    'Return.ReturnData.IRS990.TotalAssetsGrp.BOYAmt':'total_assets_boy',
    'Return.ReturnData.IRS990EZ.Form990TotalAssetsGrp.BOYAmt':'total_assets_boy_ez',
    'Return.ReturnData.IRS990EZ.Form990TotalAssetsGrp.EOYAmt':'total_assets_eoy_ez',
    'Return.ReturnData.IRS990ScheduleA.PublicSupportCY170Pct':'public_support_pct_cy',
    'Return.ReturnData.IRS990ScheduleA.PublicSupportPY170Pct':'public_support_pct_py',
    'Return.ReturnData.IRS990ScheduleA.PublicOrganization170Ind':'public_org_170',
    'Return.ReturnData.IRS990ScheduleA.PublicSupportTotal170Amt':'public_support_total_amount',
    'Return.ReturnData.IRS990ScheduleA.TotalSupportAmt':'total_support_amount',
}

def batch(element_list, batch_size):
    """A function to split a list into batches.
    """
    for i in range(0, len(element_list), batch_size):
        yield element_list[i:i + batch_size]



def flatten(current, key, result):
    """Flattens nested dictionaries and lists
    """
    if isinstance(current, dict):
        for k in current:
            new_key = "{0}.{1}".format(key, k) if key else k
            flatten(current[k], new_key, result)
    elif isinstance(current, list):
        for i,v in enumerate(current):
            new_key = "{0}.{1}".format(key, i) if key else str(i)
            flatten(v, new_key, result)
    else:
        result[key] = current

    return result
        

def process_chunk(files, export_path):
    """Processes list of files and stores them in a CSV
    """
    forms = []

    # Process every file
    for file in files:
        with open(file) as fd:
            try:
                processed_form = flatten(xmltodict.parse(fd.read()), '', {})
            except UnicodeDecodeError as exc:
                print("Couldn't decode file {} with error: {}".format(file, str(exc)))
                continue
            except xml.parsers.expat.ExpatError as exc:
                print("Failed to parse file {} with error: {}".format(file, str(exc)))
                continue
        filtered_form = {}
        for key_from, key_to in COLUMNS_NAME_MAP.items():
            if key_from in processed_form:
                filtered_form[key_to] = processed_form[key_from]
        forms.append(filtered_form)

    # Finding list of unique keys
    column_names = list(set(chain.from_iterable(value.keys() for value in forms)))

    # Creating dataframe from a list of dictionaries
    df = pd.DataFrame(forms, columns=column_names)
    
    # Exporting into a CSV file
    df.to_csv(export_path, encoding='utf-8', index=False)


def convert_files_to_csvs(paths, batch_size=10000):
    """Batches files and converts them from XMLs to CSVs
    """
    for i, v in enumerate(batch(paths, batch_size)):
        start = time.time()
        process_chunk(v, 'irs990_part_{}.csv'.format(i))
        finish = time.time()
        process_time = int(finish - start)
        print('Process time for batch {} is {}s'.format(i, process_time))



def get_xml_files_recrursively(dir_path):
    """Returns paths to XML files in a directory.
    """
    paths = []
    for root, _, files in os.walk(dir_path):
        for filename in files:
                if filename.endswith('.xml'):
                    paths.append(os.path.join(root, filename))
    #print('generated paths', paths)
    return paths


if __name__ == '__main__':
    directory = os.path.expanduser('/Volumes/usb_storage/')
    paths = get_xml_files_recrursively(directory)
    convert_files_to_csvs(paths, batch_size=500)


print('CONVERSION IS DONE')

