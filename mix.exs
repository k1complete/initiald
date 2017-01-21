defmodule Initiald.Mixfile do
  use Mix.Project

  def project do
    [app: :initiald,
     version: "0.0.1",
     elixir: "~> 1.1-dev",
     name: "subset of Tutorial D",
     dialyzer: [plt_add_apps: [:mnesia, :qlc], 
                flags: ["-Wunmatched_returns", 
                        "-Werror_handling", 
                        "-Wrace_conditions", 
                        "-Wunderspecs"]],
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger, :mnesia, :qlc, :stdlib]]
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
    [{:ex_doc, "~> 0.14", only: :dev, git: "https://github.com/elixir-lang/ex_doc.git"},
     {:earmark, "~> 1.0", only: :dev},
     {:qlc, "~> 1.0", github: "k1complete/qlc"},
     {:dialyxir, "~> 0.3", only: [:dev]}
    ]
  end
end
