echo "alias ll='ls -l'" >> ~/.bashrc
echo "alias ccb='clear && cargo build'" >> ~/.bashrc
echo "alias ccc='clear && cargo clippy'" >> ~/.bashrc
echo "alias ccu='clear && cargo update'" >> ~/.bashrc
echo "alias cct='clear && cargo test'" >> ~/.bashrc

sudo service valkey-server start

