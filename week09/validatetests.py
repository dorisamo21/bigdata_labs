import re 
import sys
from nose.tools import assert_equal
import scmsanalysis.source.validateclaims as val


def main(argv):
    
    test_npi()    
    test_prov_last_name()
    test_prov_first_name()
    test_gender()
    test_state()
    test__prov_type()
    test_hcpcs_code()

def test_npi():
    
    assert_equal(val.validate_npi('12345678'),True)
    assert_equal(val.validate_npi('12'),True)
    assert_equal(val.validate_npi('M12345'),False)
    assert_equal(val.validate_npi('12345F'),False)
    assert_equal(val.validate_npi(''),False)            
    return

def test_prov_last_name():
    assert_equal(val.validate_prov_last_name('JohnDoe'),True)
    assert_equal(val.validate_prov_last_name('123Miss'),False)
    assert_equal(val.validate_prov_last_name(''),False)
    return

def test_prov_first_name():
    assert_equal(val.validate_prov_first_name('JaneDoe'),True)
    assert_equal(val.validate_prov_first_name('123Miss'),False)
    assert_equal(val.validate_prov_first_name(''),False)
    return

def test_gender():
    assert_equal(val.validate_gender('M'),True)
    assert_equal(val.validate_gender('F'),True)
    assert_equal(val.validate_gender('A'),False)
    return

def test_state():
    assert_equal(val.validate_state('CT'), True)
    assert_equal(val.validate_state('KR'), False)
    assert_equal(val.validate_state('va'), True) 
    assert_equal(val.validate_state('test'), False) 
    return      

def test__prov_type():
    assert_equal(val.validate_prov_type('Pathology'),True)
    assert_equal(val.validate_prov_type('Path ology '),True)
    assert_equal(val.validate_prov_type('1bc'),False)
    return

def test_hcpcs_code():
    assert(val.validate_hcpcs_code('1234'),True)
    assert(val.validate_hcpcs_code('CODE'),True)
    assert(val.validate_hcpcs_code('12CODE34'),False)
    assert(val.validate_hcpcs_code('1234!!'),False)
    return

def test_avg_submitted_chrg_amt():
    assert(val.validate_avg_submitted_chrg_amt('35.12'),True)
    assert(val.validate_avg_submitted_chrg_amt('$35.12'),True)
    assert(val.validate_avg_submitted_chrg_amt('35,12'),False)
    assert(val.validate_avg_submitted_chrg_amt('ABCD'),False)

if __name__ == "__main__":
       
    main(sys.argv[1:])

