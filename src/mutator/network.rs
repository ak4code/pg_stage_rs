use rand::Rng;

use crate::error::Result;
use crate::mutator::MutationContext;
use crate::mutator::locale::en;

pub fn uri(ctx: &mut MutationContext) -> Result<String> {
    let max_length = ctx
        .kwargs
        .get("max_length")
        .and_then(|v| v.as_u64())
        .unwrap_or(2048) as usize;
    let unique = ctx.get_bool_kwarg("unique");

    let mut gen = || {
        let scheme = en::URI_SCHEMES[ctx.rng.gen_range(0..en::URI_SCHEMES.len())];
        let domain = en::URI_DOMAINS[ctx.rng.gen_range(0..en::URI_DOMAINS.len())];
        let path_len = ctx.rng.gen_range(4..12);
        let path: String = (0..path_len)
            .map(|_| {
                let chars = b"abcdefghijklmnopqrstuvwxyz0123456789";
                chars[ctx.rng.gen_range(0..chars.len())] as char
            })
            .collect();
        let url = format!("{}://{}/{}", scheme, domain, path);
        if url.len() > max_length {
            url[..max_length].to_string()
        } else {
            url
        }
    };

    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn ipv4(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        format!(
            "{}.{}.{}.{}",
            ctx.rng.gen_range(1..255u8),
            ctx.rng.gen_range(0..255u8),
            ctx.rng.gen_range(0..255u8),
            ctx.rng.gen_range(1..255u8),
        )
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn ipv6(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let groups: Vec<String> = (0..8)
            .map(|_| format!("{:04x}", ctx.rng.gen_range(0..0xFFFFu16)))
            .collect();
        groups.join(":")
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}
