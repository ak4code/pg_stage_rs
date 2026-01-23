pub mod en;
pub mod ru;

use rand::Rng;

use warlocks_cauldron as warlocks;

pub fn get_patronymic(_rng: &mut impl Rng) -> String {
    warlocks::RussiaSpecProvider::patronymic(None)
}
