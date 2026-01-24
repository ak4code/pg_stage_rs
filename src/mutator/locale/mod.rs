pub mod en;
pub mod ru;

use rand::Rng;

pub fn get_patronymic(rng: &mut impl Rng) -> String {
    if rng.gen_bool(0.5) {
        ru::PATRONYMICS_MALE[rng.gen_range(0..ru::PATRONYMICS_MALE.len())].to_string()
    } else {
        ru::PATRONYMICS_FEMALE[rng.gen_range(0..ru::PATRONYMICS_FEMALE.len())].to_string()
    }
}
