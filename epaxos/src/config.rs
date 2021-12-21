use std::ops::Index;

use yaml_rust::YamlLoader;

#[derive(Clone)]
pub struct Configure {
    pub(crate) peer_cnt: usize,
    pub(crate) peer: Vec<String>,
    pub(crate) index: usize,
    pub(crate) epoch: usize,
}

impl Configure {
    pub fn new(peer_cnt: usize, peer: Vec<String>, index: usize, epoch: usize) -> Self {
        if (peer_cnt % 2) == 0 {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }

        Self {
            peer_cnt,
            peer,
            index,
            epoch,
        }
    }
}

impl Index<usize> for Configure {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        &self.peer[index]
    }
}

pub trait ConfigureSrc {
    fn get_configure(&self) -> Configure;
}

/// Read Configure from regular file
pub struct YamlConfigureSrc {
    yaml: String,
}

impl YamlConfigureSrc {
    pub fn new(yaml: &str) -> Self {
        Self {
            yaml: yaml.to_owned(),
        }
    }
}

impl ConfigureSrc for YamlConfigureSrc {
    fn get_configure(&self) -> Configure {
        let yaml = YamlLoader::load_from_str(&self.yaml).unwrap();
        if yaml.len() != 1 {
            panic!("We should only pass in a yaml file");
        }

        // have checked length
        let yaml = yaml.get(0).unwrap();

        // TODO: put string to const
        let peer_cnt = yaml["peer_cnt"].as_i64().unwrap() as usize;

        let peer = yaml["peer"]
            .as_vec()
            .unwrap()
            .iter()
            .map(|y| y.as_str().unwrap().to_owned())
            .collect();

        let index = yaml["index"].as_i64().unwrap() as usize;

        let epoch: usize = yaml["epoch"].as_i64().unwrap() as usize;

        Configure {
            peer_cnt,
            peer,
            index,
            epoch,
        }
    }
}
