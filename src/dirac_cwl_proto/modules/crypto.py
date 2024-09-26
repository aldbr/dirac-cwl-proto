#!/usr/bin/env python3

import base64
import codecs
import hashlib
from pathlib import Path

import typer

app = typer.Typer()


# Caesar Cipher function
def caesar_cipher(text: str, shift: int) -> str:
    encrypted_text = []
    for char in text:
        if char.isalpha():
            # Shift within alphabet bounds
            shifted = chr((ord(char.lower()) - 97 + shift) % 26 + 97)
            encrypted_text.append(shifted if char.islower() else shifted.upper())
        else:
            encrypted_text.append(char)
    return "".join(encrypted_text)


@app.command("caesar")
def caesar_command(input_string: str, shift_value: int):
    """
    Apply Caesar cipher to the input string with a given shift.
    """
    result = caesar_cipher(input_string, shift_value)
    typer.echo(f"Caesar Cipher Result: {result}")
    Path("caesar_result.txt").write_text(result)


# Base64 Encoding function
def base64_encode(text: str) -> str:
    byte_data = text.encode("utf-8")
    base64_encoded = base64.b64encode(byte_data).decode("utf-8")
    return base64_encoded


@app.command("base64")
def base64_command(input_string: str):
    """
    Base64 encode the input string.
    """
    result = base64_encode(input_string)
    typer.echo(f"Base64 Encoded Result: {result}")
    Path("base64_result.txt").write_text(result)


# MD5 Hashing function
def md5_hash(text: str) -> str:
    md5_result = hashlib.md5(text.encode("utf-8")).hexdigest()
    return md5_result


@app.command("md5")
def md5_command(input_string: str):
    """
    Compute the MD5 hash of the input string.
    """
    result = md5_hash(input_string)
    typer.echo(f"MD5 Hash Result: {result}")
    Path("md5_result.txt").write_text(result)


# ROT13 function
def rot13_encrypt(text: str) -> str:
    rot13_result = codecs.encode(text, "rot_13")
    return rot13_result


@app.command("rot13")
def rot13_command(input_string: str):
    """
    Apply ROT13 encryption to the input string.
    """
    result = rot13_encrypt(input_string)
    typer.echo(f"ROT13 Result: {result}")
    Path("rot13_result.txt").write_text(result)


if __name__ == "__main__":
    app()
