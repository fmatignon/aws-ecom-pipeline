"""
Generates realistic customer data from the parameters in settings.py
"""

from config.settings import NUM_CUSTOMERS, START_DATE, END_DATE, COUNTRIES, CUSTOMERS_PER_DAY
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import random

fake = Faker()

# Global mapping to ensure consistency between postcodes, cities and states
postcode_locations = {}

# Global tracking for unique phone numbers
used_phone_numbers = set()

# Global tracking for unique addresses per postal code
used_addresses = {}  # Key: (country, postal_code), Value: set of addresses


def _first_available(fake_local, attr_names):
    """Return the first available faker attribute call from attr_names or None."""
    for name in attr_names:
        if hasattr(fake_local, name):
            try:
                return getattr(fake_local, name)()
            except Exception:
                # If the provider raises for some reason, try the next
                continue
    return None


def get_location_for_postcode(postcode, country, fake_local):
    """
    Returns consistent city and state for a given postcode within a country.
    Keys the mapping by (country, postcode) to avoid collisions across countries.
    Creates new mapping if postcode doesn't exist.
    """
    key = (country, postcode)
    if key not in postcode_locations:
        # Prefer methods in order of likely availability per locale
        if country == 'United States':
            state = _first_available(fake_local, ['state', 'administrative_unit', 'region'])
            city = _first_available(fake_local, ['city'])
        elif country == 'Canada':
            state = _first_available(fake_local, ['province', 'state', 'administrative_unit'])
            city = _first_available(fake_local, ['city'])
        elif country == 'Australia':
            state = _first_available(fake_local, ['state', 'administrative_unit'])
            city = _first_available(fake_local, ['city'])
        elif country in ['United Kingdom', 'Germany', 'France', 'Italy', 'Spain', 'Netherlands']:
            # These locales generally expose administrative_unit / region
            state = _first_available(fake_local, ['administrative_unit', 'region', 'state'])
            city = _first_available(fake_local, ['city'])
        elif country == 'Japan':
            state = _first_available(fake_local, ['prefecture', 'administrative_unit'])
            city = _first_available(fake_local, ['city'])
        else:
            state = None
            city = _first_available(fake_local, ['city'])

        postcode_locations[key] = {
            'city': city,
            'state': state
        }

    return postcode_locations[key]


def _generate_unique_phone(fake_local, max_attempts=10):
    """Generate a unique phone number."""
    for _ in range(max_attempts):
        phone = fake_local.phone_number()
        if phone not in used_phone_numbers:
            used_phone_numbers.add(phone)
            return phone
    # If all attempts fail, return phone with customer ID appended
    phone = fake_local.phone_number()
    used_phone_numbers.add(phone)
    return phone


def _generate_unique_address_for_postcode(country, postcode, fake_local, max_attempts=10):
    """Generate a unique address for a given country and postal code."""
    key = (country, postcode)
    if key not in used_addresses:
        used_addresses[key] = set()
    
    for _ in range(max_attempts):
        address = fake_local.street_address()
        if address not in used_addresses[key]:
            used_addresses[key].add(address)
            return address
    
    # If all attempts fail, generate with suffix
    address = fake_local.street_address()
    used_addresses[key].add(address)
    return address


def generate_customers(num_customers=NUM_CUSTOMERS):
    """
    Generate customer records with realistic distributions
    """
    customers = []
    
    for i in range(1, num_customers + 1):
        # Weighted country selection using nested COUNTRIES structure
        country = random.choices(
            list(COUNTRIES.keys()),
            weights=[COUNTRIES[c]['weight'] for c in COUNTRIES.keys()]
        )[0]
        
        # Set locale for realistic names/addresses per country
        if country == 'United States':
            fake_local = Faker('en_US')
        elif country == 'Canada':
            fake_local = Faker('en_CA')
        elif country == 'United Kingdom':
            fake_local = Faker('en_GB')
        elif country == 'Germany':
            fake_local = Faker('de_DE')
        elif country == 'France':
            fake_local = Faker('fr_FR')
        elif country == 'Italy':
            fake_local = Faker('it_IT')
        elif country == 'Spain':
            fake_local = Faker('es_ES')
        elif country == 'Netherlands':
            fake_local = Faker('nl_NL')
        elif country == 'Australia':
            fake_local = Faker('en_AU')
        elif country == 'Japan':
            fake_local = Faker('ja_JP')
        else:
            fake_local = fake
        
        # Generate signup date with realistic distribution
        # More signups in recent months
        days_ago = random.triangular(0, (END_DATE - START_DATE).days, (END_DATE - START_DATE).days * 0.3)
        signup_date = END_DATE - timedelta(days=int(days_ago))
        
        # Generate postcode first and get its location info
        postcode = fake_local.postcode()
        location = get_location_for_postcode(postcode, country, fake_local)
        
        # Generate unique phone and address
        phone = _generate_unique_phone(fake_local)
        address = _generate_unique_address_for_postcode(country, postcode, fake_local)
        
        signup_date_str = signup_date.strftime('%Y-%m-%d %H:%M:%S')
        customer = {
            'customer_id': i,
            'first_name': fake_local.first_name(),
            'last_name': fake_local.last_name(),
            'email': fake.unique.email(),
            'phone': phone,
            'country': country,
            'city': location['city'],
            'state': location['state'],
            'postal_code': postcode,
            'address': address,
            'signup_date': signup_date_str,
            'created_at': signup_date_str,
            'updated_at': signup_date_str,  # Will be updated when segment changes during order generation
            'customer_segment': None,  # Will be calculated after orders are generated
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            'gender': random.choice(['M', 'F', 'Other', None])
        }
        
        customers.append(customer)
        
        # Update progress every 1%
        pct = int((i / num_customers) * 100)
        prev_pct = int(((i - 1) / num_customers) * 100) if i > 1 else -1
        
        if pct != prev_pct or i == num_customers:
            bar_width = 50
            filled = int(bar_width * pct / 100)
            bar = '|' * filled + ' ' * (bar_width - filled)
            print(f"\r      Customers: {pct:3d}% |{bar}|", end='', flush=True)
    
    # Clear progress line using ANSI escape code
    print("\r\033[K", end='', flush=True)
    return pd.DataFrame(customers)


def generate_customers_for_date_range(
    start_date: datetime,
    end_date: datetime,
    start_customer_id: int = 1
) -> pd.DataFrame:
    """
    Generate customers for a specific date range (for unified system)
    
    Args:
        start_date: Start date for customer signups
        end_date: End date for customer signups
        start_customer_id: Starting customer ID
    
    Returns:
        DataFrame with customer records
    """
    # Calculate number of days and customers to generate
    days = (end_date - start_date).days + 1
    num_customers = int(CUSTOMERS_PER_DAY * days)
    
    if num_customers == 0:
        return pd.DataFrame()
    
    customers = []
    customer_id = start_customer_id
    
    for i in range(num_customers):
        # Weighted country selection
        country = random.choices(
            list(COUNTRIES.keys()),
            weights=[COUNTRIES[c]['weight'] for c in COUNTRIES.keys()]
        )[0]
        
        # Set locale for realistic names/addresses per country
        locale_map = {
            'United States': 'en_US',
            'Canada': 'en_CA',
            'United Kingdom': 'en_GB',
            'Germany': 'de_DE',
            'France': 'fr_FR',
            'Italy': 'it_IT',
            'Spain': 'es_ES',
            'Netherlands': 'nl_NL',
            'Australia': 'en_AU',
            'Japan': 'ja_JP'
        }
        locale = locale_map.get(country, 'en_US')
        fake_local = Faker(locale)
        
        # Generate signup date within range
        days_offset = random.uniform(0, days - 1)
        signup_date = start_date + timedelta(days=int(days_offset), hours=random.randint(0, 23))
        
        # Generate postcode and location
        postcode = fake_local.postcode()
        location = get_location_for_postcode(postcode, country, fake_local)
        
        # Generate unique phone and address
        phone = _generate_unique_phone(fake_local)
        address = _generate_unique_address_for_postcode(country, postcode, fake_local)
        
        signup_date_str = signup_date.strftime('%Y-%m-%d %H:%M:%S')
        customer = {
            'customer_id': customer_id,
            'first_name': fake_local.first_name(),
            'last_name': fake_local.last_name(),
            'email': fake.unique.email(),
            'phone': phone,
            'country': country,
            'city': location['city'],
            'state': location['state'],
            'postal_code': postcode,
            'address': address,
            'signup_date': signup_date_str,
            'created_at': signup_date_str,
            'updated_at': signup_date_str,
            'customer_segment': None,
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            'gender': random.choice(['M', 'F', 'Other', None])
        }
        
        customers.append(customer)
        customer_id += 1
    
    return pd.DataFrame(customers)
