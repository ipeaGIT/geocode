import arcpy
import os
import sys
import traceback

part = sys.argv[1]
inLocator = sys.argv[2]
fInfo = sys.argv[3]

arcpy.env.overwriteOutput = True
scratchDir = os.path.dirname(sys.argv[0])

partResult = part.replace('PartTable','PartResult')

try:
    #arcpy.GeocodeAddresses_geocoding(part,inLocator,fInfo,partResult)
    arcpy.geocoding.GeocodeAddresses(part,inLocator,fInfo,partResult, "STATIC", None, "ROUTING_LOCATION", "Subaddress;'Point Address';'Street Address';'Distance Marker';Intersection;'Street Name';'Primary Postal';'Postal Locality';'Postal Extension';LatLong;XY;YX;MGRS;USNG;Block;Sector;Neighborhood;District;City;'Metro Area';Subregion;Region;Territory;Country;Zone;'Amusement Park';Aquarium;'Art Gallery';'Art Museum';Billiards;'Bowling Alley';Casino;Cinema;'Historical Monument';'History Museum';'Indoor Sports';'Jazz Club';Landmark;'Live Music';Museum;'Other Arts and Entertainment';'Performing Arts';Ruin;'Science Museum';'Tourist Attraction';'Wild Animal Park';Zoo;College;'Fine Arts School';'Other Education';School;'Vocational School';'African Food';'American Food';'Argentinean Food';'Australian Food';'Austrian Food';Bakery;'Balkan Food';'BBQ and Southern Food';'Belgian Food';Bistro;'Brazilian Food';Breakfast;Brewpub;'British Isles Food';Burgers;'Cajun and Creole Food';'Californian Food';'Caribbean Food';'Chicken Restaurant';'Chilean Food';'Chinese Food';'Coffee Shop';'Continental Food';Creperie;'East European Food';'Fast Food';'Filipino Food';Fondue;'French Food';'Fusion Food';'German Food';'Greek Food';Grill;'Hawaiian Food';'Ice Cream Shop';'Indian Food';'Indonesian Food';'International Food';'Irish Food';'Italian Food';'Japanese Food';'Korean Food';'Kosher Food';'Latin American Food';'Malaysian Food';'Mexican Food';'Middle Eastern Food';'Moroccan Food';'Other Restaurant';Pastries;Pizza;'Polish Food';'Portuguese Food';'Russian Food';'Sandwich Shop';'Scandinavian Food';Seafood;Snacks;'South American Food';'Southeast Asian Food';'Southwestern Food';'Spanish Food';'Steak House';Sushi;'Swiss Food';Tapas;'Thai Food';'Turkish Food';'Vegetarian Food';'Vietnamese Food';Winery;Atoll;Basin;Butte;Canyon;Cape;Cave;Cliff;Desert;Dune;Flat;Forest;Glacier;Grassland;Hill;Island;Isthmus;Lava;Marsh;Meadow;Mesa;Mountain;'Mountain Range';Oasis;'Other Land Feature';Peninsula;Plain;Plateau;Point;Ravine;Ridge;Rock;Scrubland;Swamp;Valley;Volcano;Wetland;'Bar or Pub';Dancing;Karaoke;'Night Club';Nightlife;Basketball;Beach;Campground;'Diving Center';Fishing;Garden;'Golf Course';'Golf Driving Range';Harbor;Hockey;'Ice Skating Rink';'Nature Reserve';'Other Parks and Outdoors';Park;Racetrack;'Scenic Overlook';'Shooting Range';'Ski Lift';'Ski Resort';Soccer;'Sports Center';'Sports Field';'Swimming Pool';'Tennis Court';Trail;'Wildlife Reserve';Ashram;'Banquet Hall';'Border Crossing';Building;'Business Facility';Cemetery;Church;'City Hall';'Civic Center';'Convention Center';'Court House';Dentist;Doctor;Embassy;Factory;Farm;'Fire Station';'Government Office';Gurdwara;Hospital;'Industrial Zone';Library;Livestock;'Medical Clinic';'Military Base';Mine;Mosque;Observatory;'Oil Facility';Orchard;'Other Professional Place';'Other Religious Place';'Place of Worship';Plantation;'Police Station';'Post Office';'Power Station';Prison;'Public Restroom';'Radio Station';Ranch;'Recreation Facility';'Religious Center';'Scientific Research';Shrine;Storage;Synagogue;Telecom;Temple;Tower;Veterinarian;Vineyard;Warehouse;'Water Tank';'Water Treatment';Estate;House;'Nursing Home';'Residential Area';ATM;'Auto Dealership';'Auto Maintenance';'Auto Parts';Bank;Bookstore;Butcher;'Candy Store';'Car Wash';'Childrens Apparel';'Clothing Store';'Consumer Electronics Store';'Convenience Store';'Department Store';Electrical;'Fitness Center';'Flea Market';'Food and Beverage Shop';Footwear;'Furniture Store';'Gas Station';Grocery;'Home Improvement Store';Market;'Mens Apparel';'Mobile Phone Shop';'Motorcycle Shop';'Office Supplies Store';'Other Shops and Service';'Pet Store';Pharmacy;Plumbing;'Repair Services';'Shopping Center';Spa;'Specialty Store';'Sporting Goods Store';'Tire Store';'Toy Store';'Used Car Dealership';'Wholesale Warehouse';'Womens Apparel';Airport;'Bed and Breakfast';Bridge;'Bus Station';'Cargo Center';Dock;Ferry;Heliport;'Highway Exit';Hostel;Hotel;Marina;'Metro Station';Motel;'Other Travel';Parking;Pier;Port;'Rental Cars';Railyard;Resort;'Rest Area';Taxi;Tollbooth;'Tourist Information';'Train Station';'Transportation Service';'Truck Stop';Tunnel;'Weigh Station';Bay;Canal;Channel;Cove;Dam;Delta;Estuary;Fjord;Gulf;'Hot Spring';Irrigation;Jetty;Lagoon;Lake;Ocean;'Other Water Feature';Reef;Reservoir;Sea;Sound;Spring;Strait;Stream;Waterfall;Well;Wharf", "MINIMAL")
except:
    fd = open(os.path.join(scratchDir,'workererror.txt'),'a')
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
            str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
    fd.write(pymsg+'\n')
    msgs = "GP ERRORS:\n" + arcpy.GetMessages(2) + "\n"
    fd.write(msgs+'\n')
    fd.close()

