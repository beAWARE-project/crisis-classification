# Created: 12/03/2018
#
# Implements the 3rd algorithm of Crisis Classification module
# based on the Incident Reports
#

from Auxiliary_functions import calculate_hazard

# input fields from APP
category_field = 1
hazard_field = 0.75
#hazard_field = None
#category_field = None

if category_field == 1 or category_field == None:

    # Crisis level = hydraulic risk

    # Step a: calculate hazard
    if hazard_field != None:
        hazard = calculate_hazard( hazard_field )
        print(" hazard = ", hazard['Value'], ", ", hazard['Description'])
    else:
        print(" hazard from GIS " )
        hazard = {'Value': 0, 'Description': 'hazard from GIS '}

    # Step b: calculate vulnerability


    # Step c: calculate exposure


    # Step d: calculate Risk

    Risk = hazard['Value']
    print(Risk)

else:

    # Crisis level = Severity of incident
    print('Crisis level = Severity of incident')