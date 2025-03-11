//! 流组件模块
//!
//! 流是完整的数据处理单元，包含输入、管道和输出。

use crate::buffer::Buffer;
use crate::input::Ack;
use crate::{input::Input, output::Output, pipeline::Pipeline, Error, MessageBatch};
use flume::{Receiver, Sender};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{debug, error, info};
use waitgroup::{WaitGroup, Worker};

/// 流结构体，包含输入、管道、输出和可选的缓冲区
pub struct Stream {
    input: Arc<dyn Input>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn Output>,
    buffer: Option<Arc<dyn Buffer>>,
    thread_num: i32,
}

impl Stream {
    /// 创建一个新的流
    pub fn new(
        input: Arc<dyn Input>,
        pipeline: Pipeline,
        output: Arc<dyn Output>,
        buffer: Option<Arc<dyn Buffer>>,
        thread_num: i32,
    ) -> Self {
        Self {
            input,
            pipeline: Arc::new(pipeline),
            output,
            buffer,
            thread_num,
        }
    }

    /// 运行流处理
    pub async fn run(&mut self) -> Result<(), Error> {
        // 连接输入和输出
        self.input.connect().await?;
        self.output.connect().await?;

        let (input_sender, input_receiver) = flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(1000);
        let (buffer_sender, buffer_receiver) = flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(1000);
        let (output_sender, output_receiver) =
            flume::bounded::<(Vec<MessageBatch>, Arc<dyn Ack>)>(1000);
        let input = Arc::clone(&self.input);

        let wg = WaitGroup::new();
        // 输入
        let worker = wg.worker();
        let output_arc = self.output.clone();
        tokio::spawn(Self::do_input(input, input_sender, worker, output_arc));

        let mut processor_input = input_receiver;
        // 如果有缓冲区，则启动缓冲区处理循环
        if let Some(buffer) = &self.buffer {
            let buffer_clone = buffer.clone();
            let input_receiver_clone = processor_input.clone();
            let buffer_sender_clone = buffer_sender.clone();
            let worker = wg.worker();
            tokio::spawn(async move {
                let _worker = worker;
                Self::do_buffer(buffer_clone, input_receiver_clone, buffer_sender_clone).await;
            });

            // 缓冲区
            processor_input = buffer_receiver;
        }

        for i in 0..self.thread_num {
            let pipeline = self.pipeline.clone();
            let processor_input = processor_input.clone();
            let output_sender = output_sender.clone();
            let worker = wg.worker();
            tokio::spawn(async move {
                let _worker = worker;
                Self::do_processor(pipeline, processor_input, output_sender, i + 1).await;
            });
        }
        // 关闭发送端以通知所有工作线程
        drop(output_sender);

        loop {
            match output_receiver.recv_async().await {
                Ok(msg) => {
                    let size = &msg.0.len();
                    let mut success_cnt = 0;
                    for x in &msg.0 {
                        match self.output.write(x).await {
                            Ok(_) => {
                                success_cnt = success_cnt + 1;
                            }
                            Err(e) => {
                                error!("{}", e);
                            }
                        }
                    }

                    // 确认消息已成功处理
                    if size == &success_cnt {
                        msg.1.ack().await;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        wg.wait();

        info!("Closing......");
        self.close().await?;
        info!("close.");

        Ok(())
    }

    async fn do_input(
        input: Arc<dyn Input>,
        input_sender: Sender<(MessageBatch, Arc<dyn Ack>)>,
        _worker: Worker,
        output_arc: Arc<dyn Output>,
    ) {
        // 设置信号处理器
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");

        loop {
            tokio::select! {
                _ = sigint.recv() => {
                    info!("Received SIGINT, exiting...");
                    break;
                },
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, exiting...");
                    break;
                },
                result = input.read() =>{
                    match result {
                    Ok(msg) => {
                        debug!("Received input message: {:?}", &msg.0.as_string());
                        if let Err(e) = input_sender.send_async(msg).await {
                            error!("Failed to send input message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        match e {
                            Error::Done => {
                                // 输入完成时，关闭发送端以通知所有工作线程
                                return;
                            }
                            Error::Disconnection => loop {
                                match output_arc.connect().await {
                                    Ok(_) => {
                                        info!("input reconnected");
                                        break;
                                    }
                                    Err(e) => {
                                        error!("{}", e);
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    }
                                };
                            },
                            Error::Config(e) => {
                                error!("{}", e);
                                break;
                            }
                            _ => {
                                error!("{}", e);
                            }
                        };
                    }
                    };
                }
            }
        }
        info!("input stopped");
    }

    async fn do_buffer(
        buffer_clone: Arc<dyn Buffer>,
        input_receiver_clone: Receiver<(MessageBatch, Arc<dyn Ack>)>,
        buffer_sender_clone: Sender<(MessageBatch, Arc<dyn Ack>)>,
    ) {
        info!("Buffer processor started");
        loop {
            match input_receiver_clone.recv_async().await {
                Ok((msg, ack)) => {
                    // 将消息推入缓冲区
                    debug!("Pushing message to buffer: {:?}", &msg.as_string());
                    if let Err(e) = buffer_clone.push(&msg).await {
                        error!("Failed to push message to buffer: {}", e);
                        continue;
                    }

                    // 从缓冲区弹出消息并发送到处理管道
                    match buffer_clone.pop().await {
                        Ok(Some(buffered_msg)) => {
                            debug!(
                                "Popped message from buffer: {:?}",
                                &buffered_msg.as_string()
                            );
                            if let Err(e) =
                                buffer_sender_clone.send_async((buffered_msg, ack)).await
                            {
                                error!("Failed to send buffered message: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            // // 缓冲区为空，直接发送原始消息
                            // if let Err(e) = buffer_sender_clone.send_async((msg, ack)).await {
                            //     error!("Failed to send message: {}", e);
                            //     break;
                            // }
                        }
                        Err(e) => {
                            error!("Failed to pop message from buffer: {}", e);
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        info!("Buffer processor stopped");
    }
    async fn do_processor(
        pipeline: Arc<Pipeline>,
        input_receiver: Receiver<(MessageBatch, Arc<dyn Ack>)>,
        output_sender: Sender<(Vec<MessageBatch>, Arc<dyn Ack>)>,
        i: i32,
    ) {
        info!("Worker {} started", i);
        loop {
            match input_receiver.recv_async().await {
                Ok((msg, ack)) => {
                    // 通过管道处理消息
                    debug!("Processing input message: {:?}", &msg.as_string());
                    let processed = pipeline.process(msg).await;

                    // 处理结果消息
                    match processed {
                        Ok(msgs) => {
                            for x in &msgs {
                                debug!("Processing output message: {:?}", x.as_string());
                            }

                            if let Err(e) = output_sender.send_async((msgs, ack)).await {
                                error!("Failed to send processed message: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("{}", e)
                        }
                    }
                }
                Err(_e) => {
                    break;
                }
            }
        }
        info!("Worker {} stopped", i);
    }
    pub async fn close(&mut self) -> Result<(), Error> {
        // 关闭顺序：输入 -> 管道 -> 缓冲区 -> 输出
        self.input.close().await?;
        self.pipeline.close().await?;

        // 关闭缓冲区（如果存在）
        if let Some(buffer) = &self.buffer {
            buffer.close().await?;
        }

        self.output.close().await?;
        Ok(())
    }
}

/// 流配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    pub input: crate::input::InputConfig,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub output: crate::output::OutputConfig,
    pub buffer: Option<crate::buffer::BufferConfig>,
}

impl StreamConfig {
    /// 根据配置构建流
    pub fn build(&self) -> Result<Stream, Error> {
        let input = self.input.build()?;
        let (pipeline, thread_num) = self.pipeline.build()?;
        let output = self.output.build()?;
        let buffer = if let Some(buffer_config) = &self.buffer {
            let arc = buffer_config.build()?;
            Some(arc)
        } else {
            None
        };

        Ok(Stream::new(input, pipeline, output, buffer, thread_num))
    }
}
