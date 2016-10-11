defmodule Initiald.Mixfile do
  use Mix.Project

  def project do
    [app: :initiald,
     version: "0.0.1",
     elixir: "~> 1.1-dev",
     name: "subset of Tutorial D",
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [{:ex_doc, "~> 0.7", only: :dev, git: "https://github.com/elixir-lang/ex_doc.git"},
     {:earmark, "~> 0.1", only: :dev},
     {:qlc, "~> 1.0", path: "../qlc"}
    ]
  end
end
