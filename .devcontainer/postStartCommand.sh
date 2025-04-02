#!/bin/bash
# filepath: /workspaces/emr_data_pipeline/.devcontainer/postStartCommand.sh


# Store the export command in a variable
ENV_EXPORT="export $(grep -v '^#' /workspaces/emr_data_pipeline/.env | xargs)"
# Append the export command to .bashrc only if it doesn't already exist
grep -qF "$ENV_EXPORT" ~/.bashrc || echo "$ENV_EXPORT" >> ~/.bashrc

# Set the timezone
sudo ln -sf /usr/share/zoneinfo/Europe/Madrid /etc/localtime