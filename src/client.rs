use std::collections::HashMap;

use tonic::transport::Channel;

pub mod mr {
    tonic::include_proto!("mapreduce");
}

pub struct Client {
    pub inner: mr::map_reduce_client::MapReduceClient<Channel>,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = mr::map_reduce_client::MapReduceClient::connect(addr.to_string()).await?;
        Ok(Client { inner: client })
    }

    pub async fn get_task(&mut self) -> Result<TaskType, Box<dyn std::error::Error>> {
        let response = self.inner.get_task(mr::Empty {}).await?.into_inner();

        let task_type = match response.task_type.as_str() {
            "map" => TaskType::Map {
                task_id: response.task_id,
                input_files: response.input_files,
                n_reduce: response.n_reduce,
                output_path: response.output_path,
            },
            "reduce" => TaskType::Reduce {
                task_id: response.task_id,
                input_files: response.input_files,
                n_reduce: response.n_reduce,
                output_path: response.output_path,
            },
            "idle" => TaskType::Idle,
            "exit" => TaskType::Exit,
            _ => TaskType::Idle,
        };

        Ok(task_type)
    }

    pub async fn map_done(
        &mut self,
        task_id: u32,
        files: HashMap<u32, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = mr::MapDoneRequest {
            task_id,
            files: files.into_iter().collect(),
        };
        self.inner.map_done(request).await?;
        Ok(())
    }

    pub async fn reduce_done(&mut self, task_id: u32) -> Result<(), Box<dyn std::error::Error>> {
        let request = mr::ReduceDoneRequest { task_id };
        self.inner.reduce_done(request).await?;
        Ok(())
    }
}

pub enum TaskType {
    Map {
        task_id: u32,
        input_files: Vec<String>,
        n_reduce: u32,
        output_path: String,
    },
    Reduce {
        task_id: u32,
        input_files: Vec<String>,
        n_reduce: u32,
        output_path: String,
    },
    Idle,
    Exit,
}
