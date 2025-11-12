"""
Realistic categories, sub-categories, product names, brands, price ranges and available colors.
"""
PRODUCT_TEMPLATES = {
    'Electronics': {
        'Laptops': {
            'brands': ['TechPro', 'ElectroMax', 'ByteWave'],
            'adjectives': ['Premium', 'Pro', 'Elite', 'Ultra', 'Advanced', 'Professional'],
            'margin_range': (0.15, 0.35),  # 15-35% margin
            'products': [
                {'name': 'Ultrabook', 'price_range': (700, 1400), 'colors': ['Silver', 'Space Gray', 'Black', 'White']},
                {'name': 'Gaming Laptop', 'price_range': (1100, 2500), 'colors': ['Black', 'Gray', 'Red', 'Blue']},
                {'name': '2-in-1 Laptop', 'price_range': (800, 1800), 'colors': ['Silver', 'Black', 'Rose Gold', 'Blue']},
                {'name': 'Business Laptop', 'price_range': (850, 1700), 'colors': ['Black', 'Silver', 'Gray', 'Navy']}
            ]
        },
        'Phones': {
            'brands': ['TechPro', 'ElectroMax', 'ByteWave', 'DigitalEdge'],
            'adjectives': ['Smart', 'Pro', 'Max', 'Ultra', 'Plus', 'Elite'],
            'margin_range': (0.20, 0.40),  # 20-40% margin
            'products': [
                {'name': '5G Smartphone', 'price_range': (600, 1200), 'colors': ['Black', 'Silver', 'Blue', 'White', 'Rose Gold']},
                {'name': 'Budget Smartphone', 'price_range': (200, 500), 'colors': ['Black', 'Blue', 'Red', 'White']},
                {'name': 'Flagship Smartphone', 'price_range': (900, 1500), 'colors': ['Black', 'Silver', 'Space Gray', 'Blue', 'Purple']},
                {'name': 'Foldable Phone', 'price_range': (1200, 2200), 'colors': ['Black', 'Silver', 'Gold', 'Gray']}
            ]
        },
        'Tablets': {
            'brands': ['TechPro', 'ElectroMax', 'ByteWave'],
            'adjectives': ['Pro', 'Air', 'Plus', 'Max', 'Elite', 'Premium'],
            'margin_range': (0.18, 0.38),  # 18-38% margin
            'products': [
                {'name': '10-inch Tablet', 'price_range': (350, 900), 'colors': ['Silver', 'Space Gray', 'Black', 'Blue']},
                {'name': 'Kids Tablet', 'price_range': (100, 299), 'colors': ['Blue', 'Pink', 'Green', 'Yellow', 'Red']},
                {'name': 'Pro Tablet', 'price_range': (900, 1700), 'colors': ['Space Gray', 'Silver', 'Black']},
                {'name': 'E-Reader Tablet', 'price_range': (70, 250), 'colors': ['Black', 'White', 'Gray']}
            ]
        },
        'Audio': {
            'brands': ['TechPro', 'ElectroMax', 'DigitalEdge'],
            'adjectives': ['Wireless', 'Pro', 'Premium', 'Elite', 'Studio', 'Hi-Fi'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Wireless Earbuds', 'price_range': (30, 250), 'colors': ['Black', 'White', 'Blue', 'Red', 'Pink']},
                {'name': 'Noise Cancelling Headphones', 'price_range': (120, 550), 'colors': ['Black', 'Silver', 'Blue', 'White', 'Red']},
                {'name': 'Bluetooth Speaker', 'price_range': (40, 320), 'colors': ['Black', 'Blue', 'Red', 'Gray', 'Teal']},
                {'name': 'Soundbar', 'price_range': (100, 600), 'colors': ['Black', 'Silver', 'Gray', 'White']}
            ]
        },
        'Cameras': {
            'brands': ['TechPro', 'ElectroMax'],
            'adjectives': ['Pro', 'Elite', 'Professional', 'Advanced', 'Studio', 'Premium'],
            'margin_range': (0.15, 0.35),  # 15-35% margin
            'products': [
                {'name': 'Mirrorless Camera', 'price_range': (500, 2200), 'colors': ['Black', 'Silver']},
                {'name': 'DSLR Camera', 'price_range': (400, 2500), 'colors': ['Black', 'Silver']},
                {'name': 'Action Camera', 'price_range': (150, 500), 'colors': ['Black', 'White', 'Orange', 'Blue', 'Yellow']},
                {'name': 'Vlogging Camera', 'price_range': (300, 900), 'colors': ['Black', 'Silver', 'White', 'Pink']}
            ]
        },
        'Accessories': {
            'brands': ['TechPro', 'ElectroMax', 'ByteWave', 'SmartTech', 'HomeTech'],
            'adjectives': ['Smart', 'Pro', 'Fast', 'Premium', 'Ultra', 'Advanced'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'USB-C Hub', 'price_range': (20, 70), 'colors': ['Black', 'Silver', 'Gray', 'White']},
                {'name': 'Portable Charger', 'price_range': (15, 150), 'colors': ['Black', 'White', 'Blue', 'Red', 'Gray']},
                {'name': 'Smart Home Hub', 'price_range': (60, 300), 'colors': ['White', 'Black', 'Gray']},
                {'name': 'Wireless Charger', 'price_range': (25, 90), 'colors': ['Black', 'White', 'Silver', 'Blue']}
            ]
        }
    },
    'Clothing': {
        'Men': {
            'brands': ['StyleCo', 'FashionLine', 'ClassicFit', 'UrbanWear', 'ActiveWear'],
            'adjectives': ['Classic', 'Modern', 'Premium', 'Elite', 'Signature', 'Essential'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Performance Polo', 'price_range': (25, 90), 'colors': ['Navy', 'Black', 'White', 'Gray', 'Navy Blue', 'Charcoal']},
                {'name': 'Slim Fit Jeans', 'price_range': (40, 120), 'colors': ['Dark Blue', 'Black', 'Light Blue', 'Gray', 'Indigo']},
                {'name': 'Lightweight Jacket', 'price_range': (50, 180), 'colors': ['Black', 'Navy', 'Olive', 'Gray', 'Brown', 'Charcoal']},
                {'name': 'Casual Button-Up', 'price_range': (30, 100), 'colors': ['White', 'Blue', 'Gray', 'Navy', 'Black', 'Beige']}
            ]
        },
        'Women': {
            'brands': ['StyleCo', 'FashionLine', 'ClassicFit', 'TrendSet'],
            'adjectives': ['Elegant', 'Chic', 'Premium', 'Designer', 'Signature', 'Luxury'],
            'margin_range': (0.45, 0.65),  # 45-65% margin
            'products': [
                {'name': 'Wrap Dress', 'price_range': (45, 180), 'colors': ['Black', 'Navy', 'Burgundy', 'Teal', 'Pink', 'Floral Print']},
                {'name': 'High-Rise Leggings', 'price_range': (20, 75), 'colors': ['Black', 'Navy', 'Gray', 'Burgundy', 'Olive', 'Charcoal']},
                {'name': 'Wool Coat', 'price_range': (85, 320), 'colors': ['Black', 'Camel', 'Gray', 'Navy', 'Beige', 'Burgundy']},
                {'name': 'Silk Blouse', 'price_range': (40, 130), 'colors': ['White', 'Black', 'Navy', 'Pink', 'Burgundy', 'Ivory']}
            ]
        },
        'Kids': {
            'brands': ['StyleCo', 'FashionLine', 'ClassicFit'],
            'adjectives': ['Fun', 'Colorful', 'Playful', 'Comfortable', 'Durable', 'Classic'],
            'margin_range': (0.35, 0.55),  # 35-55% margin
            'products': [
                {'name': 'Graphic Tee', 'price_range': (10, 32), 'colors': ['Red', 'Blue', 'Yellow', 'Green', 'Pink', 'Orange', 'Purple']},
                {'name': 'Stretch Denim', 'price_range': (18, 60), 'colors': ['Blue', 'Black', 'Light Blue', 'Gray']},
                {'name': 'Rain Jacket', 'price_range': (20, 95), 'colors': ['Yellow', 'Blue', 'Red', 'Green', 'Pink', 'Orange']},
                {'name': 'Play Shorts', 'price_range': (12, 38), 'colors': ['Navy', 'Khaki', 'Red', 'Blue', 'Green', 'Gray']}
            ]
        },
        'Shoes': {
            'brands': ['UrbanWear', 'ActiveWear', 'ActiveLife', 'AthleticEdge'],
            'adjectives': ['Performance', 'Pro', 'Elite', 'Premium', 'Athletic', 'Advanced'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Running Sneakers', 'price_range': (45, 150), 'colors': ['Black', 'White', 'Gray', 'Navy', 'Red', 'Blue']},
                {'name': 'Leather Boots', 'price_range': (65, 220), 'colors': ['Brown', 'Black', 'Tan', 'Gray', 'Navy']},
                {'name': 'Canvas Slip-Ons', 'price_range': (20, 80), 'colors': ['Black', 'White', 'Navy', 'Red', 'Gray', 'Blue']},
                {'name': 'Trail Shoes', 'price_range': (50, 175), 'colors': ['Black', 'Gray', 'Olive', 'Brown', 'Navy', 'Orange']}
            ]
        },
        'Accessories': {
            'brands': ['StyleCo', 'FashionLine', 'ClassicFit', 'TrendSet', 'LifestyleCo'],
            'adjectives': ['Classic', 'Designer', 'Premium', 'Signature', 'Luxury', 'Essential'],
            'margin_range': (0.50, 0.70),  # 50-70% margin
            'products': [
                {'name': 'Woven Belt', 'price_range': (12, 48), 'colors': ['Brown', 'Black', 'Tan', 'Navy', 'Gray']},
                {'name': 'Wool Scarf', 'price_range': (20, 90), 'colors': ['Gray', 'Navy', 'Burgundy', 'Camel', 'Black', 'Plaid']},
                {'name': 'Leather Wallet', 'price_range': (30, 120), 'colors': ['Brown', 'Black', 'Tan', 'Navy']},
                {'name': 'Aviator Sunglasses', 'price_range': (35, 180), 'colors': ['Black', 'Brown', 'Gold', 'Silver', 'Tortoise']}
            ]
        }
    },
    'Home & Garden': {
        'Furniture': {
            'brands': ['HomeEssentials', 'GardenPro', 'CozyHome', 'ComfortLiving', 'PureLiving', 'LifestyleCo'],
            'adjectives': ['Comfortable', 'Elegant', 'Modern', 'Classic', 'Premium', 'Luxury'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'Sectional Sofa', 'price_range': (700, 2500), 'colors': ['Gray', 'Beige', 'Navy', 'Brown', 'Black', 'Cream']},
                {'name': 'Extendable Dining Table', 'price_range': (350, 1200), 'colors': ['Brown', 'Oak', 'Walnut', 'Black', 'White', 'Espresso']},
                {'name': 'Ergonomic Desk Chair', 'price_range': (120, 600), 'colors': ['Black', 'Gray', 'Navy', 'Blue', 'White']},
                {'name': 'Storage Ottoman', 'price_range': (80, 350), 'colors': ['Brown', 'Gray', 'Navy', 'Beige', 'Black', 'Cream']}
            ]
        },
        'Kitchen': {
            'brands': ['HomeEssentials', 'GardenPro', 'CozyHome', 'SmartTech', 'HomeTech'],
            'adjectives': ['Professional', 'Premium', 'Smart', 'Stainless', 'Elite', 'Advanced'],
            'margin_range': (0.35, 0.55),  # 35-55% margin
            'products': [
                {'name': 'Stainless Cookware Set', 'price_range': (85, 450), 'colors': ['Stainless Steel', 'Black', 'Copper']},
                {'name': 'Chef Knife Set', 'price_range': (45, 300), 'colors': ['Stainless Steel', 'Black Handle', 'Wood Handle']},
                {'name': 'Programmable Coffee Maker', 'price_range': (70, 250), 'colors': ['Black', 'White', 'Silver', 'Red']},
                {'name': 'Air Fryer Oven', 'price_range': (75, 400), 'colors': ['Black', 'White', 'Silver', 'Red', 'Blue']}
            ]
        },
        'Bedding': {
            'brands': ['HomeEssentials', 'CozyHome', 'ComfortLiving', 'PureLiving', 'LifestyleCo'],
            'adjectives': ['Cozy', 'Luxury', 'Organic', 'Premium', 'Comfortable', 'Plush'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Cooling Mattress Topper', 'price_range': (60, 280), 'colors': ['White', 'Gray', 'Beige', 'Ivory']},
                {'name': 'All-Season Comforter', 'price_range': (50, 220), 'colors': ['White', 'Gray', 'Navy', 'Beige', 'Sage', 'Cream']},
                {'name': 'Organic Sheet Set', 'price_range': (55, 200), 'colors': ['White', 'Ivory', 'Gray', 'Beige', 'Sage', 'Navy']},
                {'name': 'Weighted Blanket', 'price_range': (80, 280), 'colors': ['Gray', 'Navy', 'Beige', 'Sage', 'Charcoal', 'Cream']}
            ]
        },
        'Decor': {
            'brands': ['HomeEssentials', 'GardenPro', 'CozyHome', 'LifestyleCo'],
            'adjectives': ['Artistic', 'Elegant', 'Modern', 'Vintage', 'Designer', 'Premium'],
            'margin_range': (0.50, 0.70),  # 50-70% margin
            'products': [
                {'name': 'Framed Canvas Art', 'price_range': (30, 200), 'colors': ['Various Prints', 'Abstract', 'Landscape', 'Portrait', 'Modern', 'Vintage']},
                {'name': 'Ceramic Planter', 'price_range': (18, 85), 'colors': ['White', 'Terracotta', 'Gray', 'Blue', 'Green', 'Beige']},
                {'name': 'Glass Table Lamp', 'price_range': (40, 180), 'colors': ['White', 'Black', 'Gold', 'Silver', 'Brass', 'Crystal']},
                {'name': 'Decorative Mirror', 'price_range': (60, 350), 'colors': ['Silver', 'Gold', 'Black', 'White', 'Brass', 'Wood Frame']}
            ]
        },
        'Tools': {
            'brands': ['HomeEssentials', 'GardenPro', 'SmartTech', 'HomeTech'],
            'adjectives': ['Professional', 'Heavy-Duty', 'Smart', 'Premium', 'Pro', 'Advanced'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'Cordless Drill Kit', 'price_range': (70, 300), 'colors': ['Black', 'Yellow', 'Red', 'Blue']},
                {'name': 'Multi-Tool Set', 'price_range': (25, 160), 'colors': ['Black', 'Silver', 'Red', 'Blue']},
                {'name': 'Electric Pressure Washer', 'price_range': (120, 450), 'colors': ['Yellow', 'Red', 'Black', 'Orange']},
                {'name': 'Smart Thermostat', 'price_range': (90, 300), 'colors': ['White', 'Black', 'Silver']}
            ]
        }
    },
    'Sports': {
        'Fitness': {
            'brands': ['FitGear', 'SportsPro', 'PowerFit', 'DigitalEdge', 'ActiveLife', 'AthleticEdge'],
            'adjectives': ['Professional', 'Elite', 'Pro', 'Advanced', 'Premium', 'Performance'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'Adjustable Dumbbell Set', 'price_range': (80, 250), 'colors': ['Black', 'Gray', 'Silver', 'Red']},
                {'name': 'Resistance Band Kit', 'price_range': (15, 50), 'colors': ['Black', 'Blue', 'Red', 'Yellow', 'Green', 'Purple']},
                {'name': 'Rowing Machine', 'price_range': (300, 1200), 'colors': ['Black', 'Gray', 'Silver', 'Blue']},
                {'name': 'Exercise Bike', 'price_range': (200, 900), 'colors': ['Black', 'Gray', 'Silver', 'Red', 'Blue']}
            ]
        },
        'Outdoor': {
            'brands': ['FitGear', 'SportsPro', 'PowerFit', 'ActiveLife'],
            'adjectives': ['Durable', 'Weatherproof', 'Adventure', 'Premium', 'Pro', 'Rugged'],
            'margin_range': (0.35, 0.55),  # 35-55% margin
            'products': [
                {'name': 'Inflatable Paddleboard', 'price_range': (250, 700), 'colors': ['Blue', 'Teal', 'White', 'Gray', 'Orange']},
                {'name': 'Camping Tent', 'price_range': (75, 350), 'colors': ['Green', 'Gray', 'Blue', 'Orange', 'Brown']},
                {'name': 'Hiking Backpack', 'price_range': (40, 200), 'colors': ['Black', 'Gray', 'Green', 'Blue', 'Orange', 'Red']},
                {'name': 'Compact Cooler', 'price_range': (35, 150), 'colors': ['Blue', 'Red', 'Gray', 'Black', 'Orange', 'Green']}
            ]
        },
        'Team Sports': {
            'brands': ['FitGear', 'SportsPro', 'PowerFit'],
            'adjectives': ['Professional', 'Elite', 'Pro', 'Championship', 'Premium', 'Official'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Composite Basketball', 'price_range': (25, 80), 'colors': ['Orange', 'Black', 'Blue', 'Red']},
                {'name': 'Match Soccer Ball', 'price_range': (20, 70), 'colors': ['White/Black', 'Blue', 'Red', 'Yellow']},
                {'name': 'Carbon Fiber Hockey Stick', 'price_range': (90, 250), 'colors': ['Black', 'White', 'Red', 'Blue']},
                {'name': 'Pro Baseball Glove', 'price_range': (60, 220), 'colors': ['Brown', 'Black', 'Tan', 'Red']}
            ]
        },
        'Athletic Wear': {
            'brands': ['UrbanWear', 'ActiveWear', 'ActiveLife', 'AthleticEdge'],
            'adjectives': ['Performance', 'Pro', 'Elite', 'Athletic', 'Premium', 'Advanced'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Compression Leggings', 'price_range': (28, 80), 'colors': ['Black', 'Navy', 'Gray', 'Burgundy', 'Olive', 'Charcoal']},
                {'name': 'Breathable Training Tee', 'price_range': (15, 45), 'colors': ['Black', 'White', 'Gray', 'Navy', 'Red', 'Blue']},
                {'name': 'All-Weather Running Jacket', 'price_range': (45, 140), 'colors': ['Black', 'Navy', 'Gray', 'Red', 'Yellow', 'Blue']},
                {'name': 'Moisture-Wicking Shorts', 'price_range': (18, 55), 'colors': ['Black', 'Navy', 'Gray', 'Red', 'Blue', 'Charcoal']}
            ]
        }
    },
    'Books': {
        'Fiction': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'Literary Press', 'PageTurner Press'],
            'adjectives': ['Bestselling', 'Award-Winning', 'Classic', 'New', 'Popular', 'Critically Acclaimed'],
            'margin_range': (0.20, 0.40),  # 20-40% margin
            'products': [
                {'name': 'Contemporary Fiction', 'price_range': (10, 40)},
                {'name': 'Historical Fiction', 'price_range': (12, 38)},
                {'name': 'Science Fiction', 'price_range': (13, 42)},
                {'name': 'Mystery & Thriller', 'price_range': (12, 40)},
                {'name': 'Literary Fiction', 'price_range': (14, 45)}
            ]
        },
        'Non-Fiction': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'Literary Press', 'PageTurner Press'],
            'adjectives': ['Comprehensive', 'Authoritative', 'In-Depth', 'Expert', 'Detailed', 'Essential'],
            'margin_range': (0.20, 0.40),  # 20-40% margin
            'products': [
                {'name': 'Biography & Memoir', 'price_range': (15, 45)},
                {'name': 'History', 'price_range': (14, 42)},
                {'name': 'Travel', 'price_range': (12, 38)},
                {'name': 'True Crime', 'price_range': (13, 40)}
            ]
        },
        'Self-Help': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'Literary Press', 'PageTurner Press'],
            'adjectives': ['Transformative', 'Inspiring', 'Practical', 'Essential', 'Life-Changing', 'Proven'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Motivational', 'price_range': (10, 38)},
                {'name': 'Productivity', 'price_range': (12, 42)},
                {'name': 'Relationship Advice', 'price_range': (11, 40)}
            ]
        },
        'Educational': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'EduPress', 'EduToys'],
            'adjectives': ['Comprehensive', 'Complete', 'Study', 'Essential', 'Advanced', 'Expert'],
            'margin_range': (0.15, 0.35),  # 15-35% margin
            'products': [
                {'name': 'Textbooks', 'price_range': (25, 55)},
                {'name': 'Reference Guides', 'price_range': (18, 44)},
                {'name': 'Language Learning', 'price_range': (13, 36)},
                {'name': 'Test Preparation', 'price_range': (16, 53)}
            ]
        },
        "Children's": {
            'brands': ['ReadWell Publishing', 'BookHouse', 'EduPress', 'EduToys'],
            'adjectives': ['Colorful', 'Interactive', 'Fun', 'Educational', 'Illustrated', 'Engaging'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Picture Books', 'price_range': (7, 20)},
                {'name': 'Early Readers', 'price_range': (8, 24)},
                {'name': 'Bedtime Stories', 'price_range': (8, 22)},
                {'name': 'Activity Books', 'price_range': (9, 28)}
            ]
        },
        'Young Reader': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'Literary Press', 'PageTurner Press'],
            'adjectives': ['Bestselling', 'Award-Winning', 'Popular', 'New', 'Engaging', 'Captivating'],
            'margin_range': (0.20, 0.40),  # 20-40% margin
            'products': [
                {'name': 'Middle Grade Fiction', 'price_range': (9, 25)},
                {'name': 'Young Adult Romance', 'price_range': (10, 25)},
                {'name': 'Fantasy & Adventure', 'price_range': (11, 26)}
            ]
        },
        'Workbooks & Study Guides': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'EduPress', 'EduToys'],
            'adjectives': ['Comprehensive', 'Complete', 'Practice', 'Study', 'Essential', 'Advanced'],
            'margin_range': (0.20, 0.40),  # 20-40% margin
            'products': [
                {'name': 'Math Workbook', 'price_range': (12, 35)},
                {'name': 'Science Workbook', 'price_range': (13, 38)},
                {'name': 'Language Arts Study Guide', 'price_range': (15, 40)},
                {'name': 'Test Prep Guide', 'price_range': (20, 60)}
            ]
        },
        'Comics & Graphic Novels': {
            'brands': ['ReadWell Publishing', 'BookHouse', 'Literary Press', 'PageTurner Press'],
            'adjectives': ['Collector\'s', 'Limited Edition', 'Classic', 'New', 'Popular', 'Award-Winning'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'Superhero Comics', 'price_range': (9, 30)},
                {'name': 'Manga', 'price_range': (10, 25)},
                {'name': 'Graphic Memoirs', 'price_range': (12, 28)},
                {'name': 'Fantasy Graphic Novel', 'price_range': (13, 30)}
            ]
        }
    },
    'Toys': {
        'Action Figures': {
            'brands': ['PlayTime', 'FunKids', 'ToyBox', 'ImaginePlay', 'KidJoy'],
            'adjectives': ['Deluxe', 'Collector\'s', 'Premium', 'Super', 'Ultra', 'Classic'],
            'margin_range': (0.35, 0.55),  # 35-55% margin
            'products': [
                {'name': 'Hero Action Figure', 'price_range': (10, 35), 'colors': ['Red', 'Blue', 'Black', 'Yellow', 'Green', 'Orange']},
                {'name': 'Robot Warrior', 'price_range': (15, 45), 'colors': ['Silver', 'Black', 'Blue', 'Red', 'Gray']},
                {'name': 'Fantasy Figure Set', 'price_range': (20, 60), 'colors': ['Multi-Color', 'Purple', 'Blue', 'Green', 'Gold']},
                {'name': 'Space Explorer Set', 'price_range': (18, 55), 'colors': ['White', 'Silver', 'Blue', 'Black', 'Orange']}
            ]
        },
        'Board Games': {
            'brands': ['PlayTime', 'FunKids', 'ToyBox', 'ImaginePlay', 'KidJoy'],
            'adjectives': ['Classic', 'Deluxe', 'Premium', 'Family', 'Ultimate', 'Championship'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Strategy Board Game', 'price_range': (20, 55), 'colors': ['Multi-Color', 'Blue', 'Red', 'Green', 'Brown']},
                {'name': 'Family Trivia Game', 'price_range': (15, 40), 'colors': ['Multi-Color', 'Blue', 'Red', 'Yellow', 'Green']},
                {'name': 'Cooperative Adventure Game', 'price_range': (25, 60), 'colors': ['Multi-Color', 'Purple', 'Blue', 'Green', 'Gold']},
                {'name': 'Card Battle Set', 'price_range': (10, 30), 'colors': ['Red', 'Blue', 'Black', 'Gold', 'Silver']}
            ]
        },
        'Educational': {
            'brands': ['PlayTime', 'FunKids', 'ToyBox', 'EduPress', 'EduToys'],
            'adjectives': ['STEM', 'Advanced', 'Interactive', 'Educational', 'Pro', 'Complete'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'STEM Experiment Kit', 'price_range': (25, 65), 'colors': ['Blue', 'Red', 'Yellow', 'Green', 'Multi-Color']},
                {'name': 'Coding Robot', 'price_range': (30, 90), 'colors': ['Blue', 'Red', 'Yellow', 'Green', 'Orange', 'Purple']},
                {'name': 'Math Puzzle Set', 'price_range': (12, 35), 'colors': ['Blue', 'Red', 'Yellow', 'Green', 'Multi-Color']},
                {'name': 'Language Learning Game', 'price_range': (15, 40), 'colors': ['Blue', 'Red', 'Yellow', 'Green', 'Orange']}
            ]
        },
        'Outdoor': {
            'brands': ['PlayTime', 'FunKids', 'ToyBox', 'ImaginePlay', 'KidJoy'],
            'adjectives': ['Durable', 'Weatherproof', 'Adventure', 'Premium', 'Safe', 'Fun'],
            'margin_range': (0.35, 0.55),  # 35-55% margin
            'products': [
                {'name': 'Kids Scooter', 'price_range': (35, 85), 'colors': ['Red', 'Blue', 'Pink', 'Green', 'Yellow', 'Orange']},
                {'name': 'Backyard Playset', 'price_range': (100, 350), 'colors': ['Natural Wood', 'Green', 'Blue', 'Red', 'Multi-Color']},
                {'name': 'Bubble Machine', 'price_range': (12, 32), 'colors': ['Blue', 'Pink', 'Yellow', 'Green', 'Red', 'Purple']},
                {'name': 'Water Blaster Pack', 'price_range': (18, 45), 'colors': ['Blue', 'Orange', 'Yellow', 'Green', 'Red', 'Multi-Color']}
            ]
        }
    },
    'Health & Beauty': {
        'Skincare': {
            'brands': ['GlowLabs', 'PureBeauty', 'RadiantCo', 'TrendSet', 'LifestyleCo'],
            'adjectives': ['Luxury', 'Premium', 'Organic', 'Natural', 'Advanced', 'Professional'],
            'margin_range': (0.50, 0.70),  # 50-70% margin
            'products': [
                {'name': 'Hydrating Serum', 'price_range': (22, 60)},
                {'name': 'Mineral Sunscreen', 'price_range': (15, 35)},
                {'name': 'Restorative Night Cream', 'price_range': (28, 70)},
                {'name': 'Clarifying Cleanser', 'price_range': (12, 28)}
            ]
        },
        'Haircare': {
            'brands': ['GlowLabs', 'PureBeauty', 'RadiantCo', 'TrendSet'],
            'adjectives': ['Nourishing', 'Premium', 'Organic', 'Professional', 'Luxury', 'Natural'],
            'margin_range': (0.45, 0.65),  # 45-65% margin
            'products': [
                {'name': 'Nourishing Shampoo', 'price_range': (8, 24)},
                {'name': 'Volumizing Conditioner', 'price_range': (8, 24)},
                {'name': 'Heat Protectant Spray', 'price_range': (10, 28)},
                {'name': 'Curl Defining Cream', 'price_range': (12, 30)}
            ]
        },
        'Makeup': {
            'brands': ['GlowLabs', 'PureBeauty', 'RadiantCo', 'TrendSet'],
            'adjectives': ['Longwear', 'Premium', 'Professional', 'Luxury', 'Matte', 'Glowing'],
            'margin_range': (0.55, 0.75),  # 55-75% margin
            'products': [
                {'name': 'Longwear Foundation', 'price_range': (18, 50), 'colors': ['Fair', 'Light', 'Medium', 'Tan', 'Deep', 'Various Shades']},
                {'name': 'Eyeshadow Palette', 'price_range': (16, 45), 'colors': ['Neutral', 'Warm', 'Cool', 'Smoky', 'Colorful', 'Nude']},
                {'name': 'Matte Lipstick Set', 'price_range': (14, 35), 'colors': ['Red', 'Pink', 'Nude', 'Berry', 'Coral', 'Burgundy']},
                {'name': 'Brow Sculpting Kit', 'price_range': (13, 38), 'colors': ['Blonde', 'Brown', 'Dark Brown', 'Black', 'Auburn', 'Taupe']}
            ]
        },
        'Supplements': {
            'brands': ['GlowLabs', 'PureBeauty', 'RadiantCo', 'VitalBloom', 'WellnessWorks'],
            'adjectives': ['Premium', 'Organic', 'Natural', 'Advanced', 'Complete', 'Essential'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Vitamin C Gummies', 'price_range': (10, 25)},
                {'name': 'Collagen Peptides', 'price_range': (20, 45)},
                {'name': 'Plant-Based Protein Powder', 'price_range': (25, 50)},
                {'name': 'Daily Multivitamin', 'price_range': (12, 30)}
            ]
        },
        'Fragrances': {
            'brands': ['GlowLabs', 'PureBeauty', 'RadiantCo', 'TrendSet', 'LifestyleCo'],
            'adjectives': ['Luxury', 'Premium', 'Signature', 'Elegant', 'Exotic', 'Classic'],
            'margin_range': (0.60, 0.80),  # 60-80% margin
            'products': [
                {'name': 'Eau de Parfum', 'price_range': (35, 120)},
                {'name': 'Citrus Cologne', 'price_range': (22, 75)},
                {'name': 'Travel Perfume Set', 'price_range': (18, 60)},
                {'name': 'Aromatic Body Mist', 'price_range': (12, 30)}
            ]
        }
    },
    'Food & Beverages': {
        'Snacks': {
            'brands': ['GourmetLane', 'FreshHarvest', 'ArtisanPantry', 'NatureBite', 'VitalBloom', 'WellnessWorks'],
            'adjectives': ['Gourmet', 'Artisan', 'Organic', 'Premium', 'Natural', 'Healthy'],
            'margin_range': (0.20, 0.40),  # 20-40% margin
            'products': [
                {'name': 'Protein Snack Bar', 'price_range': (8, 16)},
                {'name': 'Organic Trail Mix', 'price_range': (10, 22)},
                {'name': 'Artisan Crackers', 'price_range': (6, 15)},
                {'name': 'Gourmet Popcorn', 'price_range': (9, 18)}
            ]
        },
        'Beverages': {
            'brands': ['GourmetLane', 'FreshHarvest', 'ArtisanPantry', 'SipCraft', 'NatureBite'],
            'adjectives': ['Premium', 'Artisan', 'Craft', 'Organic', 'Natural', 'Gourmet'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Cold Brew Coffee Concentrate', 'price_range': (12, 28)},
                {'name': 'Sparkling Mineral Water', 'price_range': (10, 22)},
                {'name': 'Herbal Tea Sampler', 'price_range': (7, 20)},
                {'name': 'Craft Soda Variety Pack', 'price_range': (11, 24)}
            ]
        },
        'Pantry': {
            'brands': ['GourmetLane', 'FreshHarvest', 'ArtisanPantry', 'NatureBite'],
            'adjectives': ['Premium', 'Artisan', 'Imported', 'Gourmet', 'Organic', 'Fine'],
            'margin_range': (0.30, 0.50),  # 30-50% margin
            'products': [
                {'name': 'Extra Virgin Olive Oil', 'price_range': (14, 30)},
                {'name': 'Imported Pasta', 'price_range': (6, 16)},
                {'name': 'Gourmet Spice Blend', 'price_range': (8, 22)},
                {'name': 'Artisan Honey', 'price_range': (12, 28)}
            ]
        },
        'Gourmet': {
            'brands': ['GourmetLane', 'FreshHarvest', 'ArtisanPantry'],
            'adjectives': ['Premium', 'Luxury', 'Fine', 'Artisan', 'Gourmet', 'Exclusive'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Truffle Gift Set', 'price_range': (32, 75)},
                {'name': 'Charcuterie Collection', 'price_range': (28, 65)},
                {'name': 'Fine Chocolate Assortment', 'price_range': (15, 40)},
                {'name': 'Premium Cheese Sampler', 'price_range': (24, 55)}
            ]
        },
        'Organic': {
            'brands': ['FreshHarvest', 'NatureBite', 'VitalBloom', 'WellnessWorks'],
            'adjectives': ['Certified Organic', 'Natural', 'Pure', 'Organic', 'Premium', 'Healthy'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Certified Organic Granola', 'price_range': (10, 22)},
                {'name': 'Organic Dried Fruit Mix', 'price_range': (9, 20)},
                {'name': 'Organic Nut Butter', 'price_range': (11, 25)},
                {'name': 'Organic Quinoa Pack', 'price_range': (10, 22)}
            ]
        }
    },
    'Pet Supplies': {
        'Food': {
            'brands': ['HappyPaws', 'PetNest', 'TailTreats', 'FurryFriends', 'WhiskerWorks'],
            'adjectives': ['Premium', 'Natural', 'Grain-Free', 'Organic', 'Healthy', 'Complete'],
            'margin_range': (0.25, 0.45),  # 25-45% margin
            'products': [
                {'name': 'Grain-Free Dog Food', 'price_range': (25, 60)},
                {'name': 'High-Protein Cat Kibble', 'price_range': (18, 50)},
                {'name': 'Freeze-Dried Raw Meal', 'price_range': (30, 75)},
                {'name': 'Organic Small Pet Pellets', 'price_range': (16, 35)}
            ]
        },
        'Toys': {
            'brands': ['HappyPaws', 'PetNest', 'TailTreats', 'FurryFriends', 'WhiskerWorks'],
            'adjectives': ['Durable', 'Interactive', 'Fun', 'Premium', 'Safe', 'Engaging'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Durable Chew Toy', 'price_range': (7, 20), 'colors': ['Red', 'Blue', 'Green', 'Yellow', 'Orange', 'Purple']},
                {'name': 'Interactive Puzzle Toy', 'price_range': (10, 28), 'colors': ['Blue', 'Green', 'Yellow', 'Red', 'Multi-Color']},
                {'name': 'Feather Wand Toy', 'price_range': (5, 12), 'colors': ['Multi-Color', 'Red', 'Blue', 'Green', 'Yellow']},
                {'name': 'Treat Dispensing Ball', 'price_range': (9, 22), 'colors': ['Blue', 'Red', 'Green', 'Yellow', 'Orange', 'Purple']}
            ]
        },
        'Health': {
            'brands': ['HappyPaws', 'PetNest', 'TailTreats', 'FurryFriends', 'WhiskerWorks'],
            'adjectives': ['Premium', 'Veterinary', 'Natural', 'Complete', 'Advanced', 'Professional'],
            'margin_range': (0.35, 0.55),  # 35-55% margin
            'products': [
                {'name': 'Joint Support Chews', 'price_range': (18, 40)},
                {'name': 'Dental Care Kit', 'price_range': (10, 25)},
                {'name': 'Flea & Tick Treatment', 'price_range': (22, 55)},
                {'name': 'Omega Supplement', 'price_range': (14, 32)}
            ]
        },
        'Accessories': {
            'brands': ['HappyPaws', 'PetNest', 'TailTreats', 'FurryFriends', 'WhiskerWorks'],
            'adjectives': ['Premium', 'Comfortable', 'Durable', 'Safe', 'Ergonomic', 'Professional'],
            'margin_range': (0.40, 0.60),  # 40-60% margin
            'products': [
                {'name': 'Adjustable Harness', 'price_range': (15, 38), 'colors': ['Black', 'Navy', 'Red', 'Blue', 'Pink', 'Gray']},
                {'name': 'Travel Pet Carrier', 'price_range': (30, 80), 'colors': ['Black', 'Gray', 'Navy', 'Red', 'Blue', 'Pink']},
                {'name': 'Memory Foam Pet Bed', 'price_range': (40, 110), 'colors': ['Gray', 'Beige', 'Navy', 'Brown', 'Tan', 'Cream']},
                {'name': 'No-Pull Leash', 'price_range': (12, 30), 'colors': ['Black', 'Navy', 'Red', 'Blue', 'Pink', 'Gray']}
            ]
        }
    }
}