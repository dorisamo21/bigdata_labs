import gzip
from matplotlib import pyplot as plt
import operator
import sys
import pandas as pd

def main(argv):
    """
    This functions reads weather file,
    calls functions to accumulate, 
    displays tornado totals by state 
    @param argv: command line arguments
    """    
    
    infile2000 = gzip.open(argv[0], 'rt')      # open gzipped file in read text mode 
    infile1980 = gzip.open(argv[1], 'rt')      # open gzipped file in read text mode 
    
    df1980 = generate_file_stats(infile1980,1980)
    df2000 = generate_file_stats(infile2000,2000)
    generate_boxplot(df2000, df1980)
    return
 
def generate_file_stats(file,year):
    state_counts = accumulate_tornadoes(file)
    sorted_state = sorted(state_counts.items(),key=operator.itemgetter(1),reverse=False)
    sorted_df = compute_5_number_summary(sorted_state,year)
    return sorted_df
      
def compute_5_number_summary(state_info,year):
    state_df = pd.DataFrame(state_info,columns=['State','Count'])
    state_df['Year'] = year

    print('[ Year:',year,']')
    print(state_df['Count'].describe())
    print()
    return state_df
    
def generate_boxplot(data1, data2):
    combined_df = pd.concat([data1,data2])
    combined_df.boxplot(column='Count', by='Year',figsize=(5,5))
    plt.title("Tornado Count in States By Year")
    plt.xlabel("Year")
    plt.ylabel("Count")
    plt.suptitle("")
    plt.show()
    
def accumulate_tornadoes(infile):
    """
    This functions takes in the weather file, parses it, 
    and builds a dict that contains the tornado counts by state,
    i.e. [state:count].
    Make sure to account for states that may have no tornados.
    @param infile: weather file object
    @return: dictionary of state tornado counts; key= state, value = tornado count for that state.
    """
    states = {'ALABAMA':0,'ALASKA':0,'ARIZONA':0,'ARKANSAS':0,'CALIFORNIA':0,'COLORADO':0,'CONNECTICUT':0,'DELAWARE':0,'FLORIDA':0,'GEORGIA':0,
              'HAWAII':0,'IDAHO':0,'ILLINOIS':0,'INDIANA':0,'IOWA':0,'KANSAS':0,'KENTUCKY':0,'LOUISIANA':0,'MAINE':0,'MARYLAND':0,'MASSACHUSETTS':0,'MICHIGAN':0,'MINNESOTA':0,'MISSISSIPPI':0,
              'MISSOURI':0,'MONTANA':0,'NEBRASKA':0,'NEVADA':0,'NEW HAMPSHIRE':0,'NEW JERSEY':0,'NEW MEXICO':0,'NEW YORK':0,'NORTH CAROLINA':0,
              'NORTH DAKOTA':0,'OHIO':0,'OKLAHOMA':0,'OREGON':0,'PENNSYLVANIA':0,'RHODE ISLAND':0,'SOUTH CAROLINA':0,'SOUTH DAKOTA':0,'TENNESSEE':0,
              'TEXAS':0,'UTAH':0,'VERMONT':0,'VIRGINIA':0,'WASHINGTON':0,'WEST VIRGINIA':0,'WISCONSIN':0,'WYOMING':0}
    for line in infile:
        data = line.split(",")        
        if "Tornado" in data[12]:
            state_name = data[8].strip('"')
            if state_name in states.keys():
                tornadocnt = states.get(state_name) + 1
                states.update({state_name:tornadocnt})
    return states

def display_state_counts(state_counts):
    """ 
    This functions takes in dictionary of state tornado counts,
    sorts them by count descending, and
    print the top 5 states and counts to the console.
    @param state_counts: dictionary of state tornado counts.
    @return: None
    """
    sorted_list = sorted(state_counts.items(),key=operator.itemgetter(1),reverse=True)
    top_five_list = sorted_list[0:5]
    print()
    print('Top 5 states and No of Tornados')
    print('===============================')

    i=1
    for tfl in top_five_list:
        print(i,' ', tfl[0],'(',tfl[1],'times)')
        i+=1
#    print(*top_five_list,sep='\n')        
    return

def build_histogram(state_counts):
    """
    This functions takes in a dictionary of (state:tornado_counts),
    It builds a list of just the counts.
    It then creates a histogram grouping the states by how many tornados they had (in blocks of 20).
    It then displays the histogram in a pop-up.
    @param state_counts: dictionary of state tornado counts.
    @return: None
    """

    tornado_counts = []
    for count in state_counts.values():
        tornado_counts.append(count)
    bins = range(0,180,20)
    plt.hist(tornado_counts, bins,histtype='bar',rwidth=0.8)
    plt.xlabel("Tornado Count Ranges")
    plt.ylabel("State Counts") 
    plt.title("State Counts by Number of Tornados")
    
    plt.show() 

if __name__ == "__main__":
       
    main(sys.argv[1:])
    
