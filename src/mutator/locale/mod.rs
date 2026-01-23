pub mod en;
pub mod ru;

use crate::types::Locale;
use rand::Rng;

pub fn get_first_name(locale: Locale, rng: &mut impl Rng) -> String {
    match locale {
        Locale::En => {
            let names = en::FIRST_NAMES;
            names[rng.gen_range(0..names.len())].to_string()
        }
        Locale::Ru => {
            if rng.gen_bool(0.5) {
                let names = ru::FIRST_NAMES_MALE;
                names[rng.gen_range(0..names.len())].to_string()
            } else {
                let names = ru::FIRST_NAMES_FEMALE;
                names[rng.gen_range(0..names.len())].to_string()
            }
        }
    }
}

pub fn get_last_name(locale: Locale, rng: &mut impl Rng) -> String {
    match locale {
        Locale::En => {
            let names = en::LAST_NAMES;
            names[rng.gen_range(0..names.len())].to_string()
        }
        Locale::Ru => {
            if rng.gen_bool(0.5) {
                let names = ru::LAST_NAMES_MALE;
                names[rng.gen_range(0..names.len())].to_string()
            } else {
                let names = ru::LAST_NAMES_FEMALE;
                names[rng.gen_range(0..names.len())].to_string()
            }
        }
    }
}

pub fn get_patronymic(rng: &mut impl Rng) -> String {
    if rng.gen_bool(0.5) {
        let names = ru::PATRONYMICS_MALE;
        names[rng.gen_range(0..names.len())].to_string()
    } else {
        let names = ru::PATRONYMICS_FEMALE;
        names[rng.gen_range(0..names.len())].to_string()
    }
}

pub fn get_email_domain(locale: Locale, rng: &mut impl Rng) -> &'static str {
    match locale {
        Locale::En => {
            let domains = en::EMAIL_DOMAINS;
            domains[rng.gen_range(0..domains.len())]
        }
        Locale::Ru => {
            let domains = ru::EMAIL_DOMAINS;
            domains[rng.gen_range(0..domains.len())]
        }
    }
}

pub fn get_address(locale: Locale, rng: &mut impl Rng) -> String {
    match locale {
        Locale::En => {
            let number = rng.gen_range(1..9999);
            let street = en::STREET_NAMES[rng.gen_range(0..en::STREET_NAMES.len())];
            let suffix = en::STREET_SUFFIXES[rng.gen_range(0..en::STREET_SUFFIXES.len())];
            let city = en::CITIES[rng.gen_range(0..en::CITIES.len())];
            let state = en::STATES[rng.gen_range(0..en::STATES.len())];
            let zip = rng.gen_range(10000..99999);
            format!("{} {} {}, {}, {} {}", number, street, suffix, city, state, zip)
        }
        Locale::Ru => {
            let city = ru::CITIES[rng.gen_range(0..ru::CITIES.len())];
            let street_type = ru::STREET_TYPES[rng.gen_range(0..ru::STREET_TYPES.len())];
            let street = ru::STREETS[rng.gen_range(0..ru::STREETS.len())];
            let house = rng.gen_range(1..200);
            let apt = rng.gen_range(1..500);
            format!("г. {}, {} {}, д. {}, кв. {}", city, street_type, street, house, apt)
        }
    }
}
